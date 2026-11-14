package platform

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	reads     int
	commits   int
	commitErr error
	closeErr  error
	block     bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.reads++
	if r.block {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	return kafka.Message{Topic: "native-topic", Partition: 1, Offset: int64(r.reads), Value: []byte("payload"), Headers: []kafka.Header{{Key: "trace", Value: []byte("123")}}}, nil
}
func (r *fakeReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	return r.commitErr
}
func (r *fakeReader) Close() error { return r.closeErr }

func TestReaderProtectsFailedOffsetAndConfirmsAtMostOnce(t *testing.T) {
	for _, action := range []string{"ack", "nack", "failed_ack", "canceled_ack"} {
		t.Run(action, func(t *testing.T) {
			native := &fakeReader{}
			r := newReader(native, "orders.created")
			defer r.Close(context.Background())
			d, err := r.Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Read(context.Background()); !errors.Is(err, eventadapter.ErrInvalidState) {
				t.Fatalf("미확인 전달 다음 메시지를 읽었습니다: %v", err)
			}
			if d.Message().EventName != "orders.created" {
				t.Fatal("구독의 논리 이름을 잃었습니다")
			}
			copy := d.Message()
			copy.Payload[0] = 'X'
			copy.Metadata["trace"] = "changed"
			if string(d.Message().Payload) != "payload" || d.Message().Metadata["trace"] != "123" {
				t.Fatal("메시지 복사본이 원본을 변경했습니다")
			}
			ctx := context.Background()
			if action == "failed_ack" {
				native.commitErr = errors.New("commit failed")
			}
			if action == "canceled_ack" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if action == "nack" {
				err = d.Nack(ctx)
			} else {
				err = d.Ack(ctx)
			}
			if action == "ack" && err != nil {
				t.Fatal(err)
			}
			if action != "ack" && err == nil {
				t.Fatal("실패 확인이 오류를 반환해야 합니다")
			}
			if action == "nack" && !errors.Is(err, eventadapter.ErrReaderInvalidated) {
				t.Fatal(err)
			}
			if err := d.Ack(context.Background()); !errors.Is(err, eventadapter.ErrInvalidState) {
				t.Fatalf("중복 확인을 허용했습니다: %v", err)
			}
			if action != "ack" {
				if _, err := r.Read(context.Background()); !errors.Is(err, eventadapter.ErrReaderInvalidated) {
					t.Fatalf("실패 offset을 넘어서 읽었습니다: %v", err)
				}
				if native.reads != 1 {
					t.Fatalf("읽기 횟수 = %d", native.reads)
				}
			} else {
				if _, err := r.Read(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReaderCloseInterruptsRead(t *testing.T) {
	r := newReader(&fakeReader{block: true}, "orders")
	result := make(chan error, 1)
	go func() { _, err := r.Read(context.Background()); result <- err }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("종료가 Read를 해제하지 않았습니다")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type fakeWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.messages = append(w.messages, messages...)
	return nil
}
func (*fakeWriter) Close() error { return nil }

func TestPublisherPreservesSerializedBytesAndMetadataOwnership(t *testing.T) {
	native := &fakeWriter{}
	p := &publisher{native: native, destinations: map[string]string{"orders": "dev-orders"}, life: newLifecycle(nil, native.Close)}
	message := eventadapter.Message{EventName: "orders", Payload: []byte{0, 255, 1}, Metadata: map[string]string{"trace": "123"}}
	if err := p.Publish(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	message.Payload[0] = 9
	message.Metadata["trace"] = "changed"
	got := native.messages[0]
	if got.Topic != "dev-orders" || got.Value[0] != 0 || string(got.Headers[0].Value) != "123" {
		t.Fatalf("입력 수명이 전송 메시지를 변경했습니다: %#v", got)
	}
	if err := p.Publish(context.Background(), eventadapter.Message{EventName: "unknown"}); !errors.Is(err, eventadapter.ErrInvalidAdapter) {
		t.Fatalf("미등록 목적지를 거절해야 합니다: %v", err)
	}
	if len(native.messages) != 1 {
		t.Fatal("미등록 목적지로 메시지를 썼습니다")
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), message); !errors.Is(err, eventadapter.ErrInvalidState) {
		t.Fatal(err)
	}
}

func TestFactoryCopiesNativeConfigurationAndDoesNotConnect(t *testing.T) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	brokers := []string{"unreachable.invalid:9092"}
	transport := &kafka.Transport{MetadataTopics: []string{"orders"}, TLS: tlsConfig}
	selected, err := NewFactory(Config{Brokers: brokers, Publish: true, PublishDestinations: testDestinations{"orders"}, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	brokers[0] = "changed"
	transport.MetadataTopics[0] = "changed"
	tlsConfig.ServerName = "changed"
	f := selected.(*factory)
	if f.config.Brokers[0] != "unreachable.invalid:9092" || f.config.Transport.MetadataTopics[0] != "orders" || f.config.Transport.TLS.ServerName != "" {
		t.Fatal("설정 복사가 원본을 공유합니다")
	}
	if err := eventadapter.ValidateFactory(selected, eventadapter.Requirements{Publish: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := selected.NewReader(context.Background(), eventadapter.Subscription{Topic: "orders"}); !errors.Is(err, eventadapter.ErrUnsupportedCapability) {
		t.Fatal(err)
	}
}

func TestReadinessRejectsNonKafkaServiceAndHonorsDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			buffer := make([]byte, 32)
			for {
				if _, err := conn.Read(buffer); err != nil {
					return
				}
			}
		}
	}()
	f, err := NewFactory(Config{Brokers: []string{listener.Addr().String()}, Consume: true, GroupID: "test", AllowInsecureTransport: true, StartupTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.NewReader(context.Background(), eventadapter.Subscription{Topic: "orders"})
	if r != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("단순 TCP 수신을 readiness로 인정했습니다: %v, %v", r, err)
	}
	<-done
}

type testDestinations []string

func (d testDestinations) PublishDestinations() []string { return d }

func TestFactoryValidatesPublishDestinations(t *testing.T) {
	for name, source := range map[string]DestinationSource{
		"missing":   nil,
		"empty":     testDestinations{},
		"invalid":   testDestinations{"orders created"},
		"duplicate": testDestinations{"orders", "orders"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewFactory(Config{Brokers: []string{"localhost:9092"}, Publish: true, PublishDestinations: source})
			if !errors.Is(err, eventadapter.ErrInvalidAdapter) {
				t.Fatalf("잘못된 목록을 거절해야 합니다: %v", err)
			}
		})
	}
}

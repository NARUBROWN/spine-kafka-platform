package platform_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	platform "github.com/NARUBROWN/spine-kafka-platform"
	eventadapter "github.com/NARUBROWN/spine/adapter/event"
	"github.com/segmentio/kafka-go"
)

func TestLiveKafkaFailedDeliveryIsRedeliveredBeforeNextMessage(t *testing.T) {
	broker := os.Getenv("SPINE_TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("SPINE_TEST_KAFKA_BROKER is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic := fmt.Sprintf("spine-platform-%d", time.Now().UnixNano())
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	defer conn.DeleteTopics(topic)
	f, err := platform.NewFactory(platform.Config{Brokers: []string{broker}, Consume: true, Publish: true, GroupID: topic, AllowInsecureTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.NewPublisher(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	first := []byte{0, 255, 1}
	for _, payload := range [][]byte{first, []byte("second")} {
		if err := p.Publish(ctx, eventadapter.Message{EventName: topic, Payload: payload, OccurredAt: time.Now(), Metadata: map[string]string{"trace": "123"}}); err != nil {
			t.Fatal(err)
		}
	}
	initCtx, cancelInit := context.WithCancel(ctx)
	r, err := f.NewReader(initCtx, eventadapter.Subscription{Topic: topic})
	cancelInit()
	if err != nil {
		t.Fatal(err)
	}
	d, err := r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.Message().Payload, first) || d.Message().Metadata["trace"] != "123" {
		t.Fatalf("메시지 왕복이 다릅니다: %#v", d.Message())
	}
	if err := d.Nack(ctx); !errors.Is(err, eventadapter.ErrReaderInvalidated) {
		t.Fatal(err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r, err = f.NewReader(ctx, eventadapter.Subscription{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	d, err = r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.Message().Payload, first) {
		t.Fatalf("실패 메시지보다 뒤의 메시지를 받았습니다: %q", d.Message().Payload)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Message().Payload) != "second" {
		t.Fatalf("다음 메시지가 다릅니다: %q", d.Message().Payload)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

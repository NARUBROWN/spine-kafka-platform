// Package platform은 Spine 이벤트 계약의 Kafka 구현입니다.
package platform

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"time"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	"github.com/segmentio/kafka-go"
)

// DestinationSource는 앱이 발행할 논리 목적지를 제공합니다.
// Spine의 이벤트 계약과 같은 메서드로 정의해 기존 계약 버전에서도 쓸 수 있습니다.
type DestinationSource interface {
	PublishDestinations() []string
}

// Config는 Kafka 연결과 활성화할 역할을 명시합니다. 생성 시 네트워크에 연결하지 않습니다.
type Config struct {
	Brokers                []string
	Consume                bool
	Publish                bool
	GroupID                string
	TopicPrefix            string
	PublishDestinations    DestinationSource
	TLS                    *tls.Config
	Dialer                 *kafka.Dialer
	Transport              *kafka.Transport
	AllowInsecureTransport bool
	StartupTimeout         time.Duration
	WriteTimeout           time.Duration
}

type factory struct {
	config       Config
	destinations map[string]string
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func resolveDestinations(config Config) (map[string]string, error) {
	if !config.Publish {
		return nil, nil
	}
	source := config.PublishDestinations
	if source == nil || nilDestinationSource(source) {
		return nil, fmt.Errorf("%w: set Config.PublishDestinations when publishing is enabled", eventadapter.ErrInvalidAdapter)
	}
	names := source.PublishDestinations()
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: Config.PublishDestinations returned no destinations", eventadapter.ErrInvalidAdapter)
	}
	destinations := make(map[string]string, len(names))
	for _, name := range names {
		topic := config.TopicPrefix + name
		if !topicName.MatchString(name) || !topicName.MatchString(topic) || topic == "." || topic == ".." {
			return nil, fmt.Errorf("%w: invalid Kafka destination %q (topic %q)", eventadapter.ErrInvalidAdapter, name, topic)
		}
		if _, exists := destinations[name]; exists {
			return nil, fmt.Errorf("%w: duplicate Kafka destination %q", eventadapter.ErrInvalidAdapter, name)
		}
		destinations[name] = topic
	}
	return destinations, nil
}

func nilDestinationSource(source DestinationSource) bool {
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// NewFactory는 설정을 복사하고 검증합니다. 브로커 연결은 NewReader 또는 NewPublisher에서 시작합니다.
func NewFactory(config Config) (eventadapter.Factory, error) {
	if len(config.Brokers) == 0 || (!config.Consume && !config.Publish) || (config.Consume && config.GroupID == "") {
		return nil, fmt.Errorf("%w: brokers, enabled roles and consumer group must be configured", eventadapter.ErrInvalidAdapter)
	}
	for _, broker := range config.Brokers {
		if broker == "" {
			return nil, fmt.Errorf("%w: broker address is empty", eventadapter.ErrInvalidAdapter)
		}
	}
	if config.StartupTimeout < 0 || config.WriteTimeout < 0 {
		return nil, fmt.Errorf("%w: timeouts cannot be negative", eventadapter.ErrInvalidAdapter)
	}
	if config.AllowInsecureTransport && (config.TLS != nil || (config.Dialer != nil && config.Dialer.TLS != nil) || (config.Transport != nil && config.Transport.TLS != nil)) {
		return nil, fmt.Errorf("%w: TLS conflicts with insecure transport", eventadapter.ErrInvalidAdapter)
	}
	destinations, err := resolveDestinations(config)
	if err != nil {
		return nil, err
	}
	// 목록을 한 번 읽어 보관하고 발행자 재생성에도 같은 경로를 사용합니다.
	config.PublishDestinations = nil
	config.Brokers = slices.Clone(config.Brokers)
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.TLS != nil {
		config.TLS = config.TLS.Clone()
	}
	dialer := *kafka.DefaultDialer
	if config.Dialer != nil {
		dialer = *config.Dialer
	}
	if dialer.TLS != nil {
		dialer.TLS = dialer.TLS.Clone()
	} else {
		dialer.TLS = effectiveTLS(config)
	}
	config.Dialer = &dialer
	config.Transport = cloneTransport(config)
	return &factory{config: config, destinations: destinations}, nil
}

func effectiveTLS(config Config) *tls.Config {
	if config.TLS != nil {
		return config.TLS.Clone()
	}
	if config.AllowInsecureTransport {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func cloneTransport(config Config) *kafka.Transport {
	transport := &kafka.Transport{}
	if original := config.Transport; original != nil {
		transport = &kafka.Transport{Dial: original.Dial, DialTimeout: original.DialTimeout,
			IdleTimeout: original.IdleTimeout, MetadataTTL: original.MetadataTTL,
			MetadataTopics: slices.Clone(original.MetadataTopics), ClientID: original.ClientID,
			TLS: original.TLS, SASL: original.SASL, Resolver: original.Resolver}
	}
	if transport.TLS != nil {
		transport.TLS = transport.TLS.Clone()
	} else {
		transport.TLS = effectiveTLS(config)
	}
	// 생성 context와 무관하게 publisher가 연결 풀의 수명을 소유합니다.
	transport.Context = context.Background()
	return transport
}

func (f *factory) Capabilities() eventadapter.Capabilities {
	return eventadapter.Capabilities{ContractVersion: eventadapter.ContractVersion, Consume: f.config.Consume, Publish: f.config.Publish}
}

func (f *factory) NewReader(ctx context.Context, subscription eventadapter.Subscription) (eventadapter.Reader, error) {
	if !f.config.Consume {
		return nil, eventadapter.ErrUnsupportedCapability
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if subscription.Topic == "" {
		return nil, fmt.Errorf("%w: subscription topic is empty", eventadapter.ErrInvalidAdapter)
	}
	topic := f.config.TopicPrefix + subscription.Topic
	if err := f.readiness(ctx, topic); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native := kafka.NewReader(kafka.ReaderConfig{Brokers: slices.Clone(f.config.Brokers), Topic: topic,
		GroupID: f.config.GroupID, Dialer: f.config.Dialer, CommitInterval: 0})
	return newReader(native, subscription.Topic), nil
}

func (f *factory) readiness(ctx context.Context, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	var failures []error
	for _, broker := range f.config.Brokers {
		conn, err := f.config.Dialer.DialContext(ctx, "tcp", broker)
		if err == nil {
			deadline, _ := ctx.Deadline()
			err = conn.SetDeadline(deadline)
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			if err == nil {
				var partitions []kafka.Partition
				partitions, err = conn.ReadPartitions(topic)
				if err == nil && len(partitions) == 0 {
					err = errors.New("topic metadata has no partitions")
				}
			}
			stop()
			_ = conn.Close()
			if err == nil && ctx.Err() == nil {
				return nil
			}
		}
		failures = append(failures, err)
		if contextErr := contextError(ctx); contextErr != nil {
			return fmt.Errorf("Kafka readiness: %w", errors.Join(err, contextErr))
		}
	}
	return fmt.Errorf("Kafka topic readiness failed: %w", errors.Join(failures...))
}

func (f *factory) NewPublisher(ctx context.Context) (eventadapter.Publisher, error) {
	if !f.config.Publish {
		return nil, eventadapter.ErrUnsupportedCapability
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	// 발행자 생성 전에 등록된 물리 토픽의 메타데이터를 확인합니다.
	startupCtx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	topics := make([]string, 0, len(f.destinations))
	for _, topic := range f.destinations {
		topics = append(topics, topic)
	}
	slices.Sort(topics)
	transport := cloneTransport(f.config)
	address := kafka.TCP(f.config.Brokers...)
	client := kafka.Client{Addr: address, Transport: transport}
	if err := checkPublishTopics(startupCtx, &client, topics); err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	native := &kafka.Writer{Addr: address, Balancer: &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, Transport: transport, WriteTimeout: f.config.WriteTimeout,
		ReadTimeout: f.config.WriteTimeout}
	p := &publisher{native: native, destinations: f.destinations}
	p.life = newLifecycle(nil, func() error { err := native.Close(); transport.CloseIdleConnections(); return err })
	return p, nil
}

func checkPublishTopics(ctx context.Context, client *kafka.Client, topics []string) error {
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return fmt.Errorf("prepare Kafka publish topics %v: %w", topics, err)
	}
	found := make(map[string]kafka.Topic, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		found[topic.Name] = topic
	}
	for _, name := range topics {
		topic := found[name]
		if topic.Error != nil {
			return fmt.Errorf("prepare Kafka publish topic %q: %w", name, topic.Error)
		}
		if len(topic.Partitions) == 0 {
			return fmt.Errorf("prepare Kafka publish topic %q: no partitions in metadata", name)
		}
	}
	return ctx.Err()
}

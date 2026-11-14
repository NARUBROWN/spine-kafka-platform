package platform

import (
	"context"
	"fmt"
	"slices"
	"sort"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	"github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type publisher struct {
	native       messageWriter
	destinations map[string]string
	life         *lifecycle
}

func (p *publisher) Publish(ctx context.Context, message eventadapter.Message) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := p.life.enter(); err != nil {
		return err
	}
	defer p.life.leave()
	topic, registered := p.destinations[message.EventName]
	if !registered {
		return fmt.Errorf("%w: Kafka publish destination %q is not registered", eventadapter.ErrInvalidAdapter, message.EventName)
	}
	keys := make([]string, 0, len(message.Metadata))
	for key := range message.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	headers := make([]kafka.Header, 0, len(keys))
	for _, key := range keys {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(message.Metadata[key])})
	}
	return p.native.WriteMessages(ctx, kafka.Message{Topic: topic,
		Value: slices.Clone(message.Payload), Time: message.OccurredAt, Headers: headers})
}

func (p *publisher) Close(ctx context.Context) error { return p.life.close(ctx) }

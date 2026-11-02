package platform

import (
	"context"
	"fmt"
	"slices"
	"sort"

	eventadapter "github.com/NARUBROWN/spine/adapter/event"
	"github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type publisher struct {
	native messageWriter
	prefix string
	life   *lifecycle
}

func (p *publisher) Publish(ctx context.Context, message eventadapter.Message) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := p.life.enter(); err != nil {
		return err
	}
	defer p.life.leave()
	if message.EventName == "" {
		return fmt.Errorf("%w: event name is empty", eventadapter.ErrInvalidAdapter)
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
	return p.native.WriteMessages(ctx, kafka.Message{Topic: p.prefix + message.EventName,
		Value: slices.Clone(message.Payload), Time: message.OccurredAt, Headers: headers})
}

func (p *publisher) Close(ctx context.Context) error { return p.life.close(ctx) }

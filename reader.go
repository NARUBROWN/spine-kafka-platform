package platform

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	eventadapter "github.com/NARUBROWN/spine/adapter/event"
	"github.com/segmentio/kafka-go"
)

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type reader struct {
	native  messageReader
	topic   string
	stop    context.Context
	life    *lifecycle
	busy    atomic.Bool
	invalid atomic.Bool
}

func newReader(native messageReader, topic string) *reader {
	stop, cancel := context.WithCancel(context.Background())
	r := &reader{native: native, topic: topic, stop: stop}
	r.life = newLifecycle(cancel, native.Close)
	return r
}

func (r *reader) Read(ctx context.Context) (eventadapter.Delivery, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.life.enter(); err != nil {
		return nil, err
	}
	defer r.life.leave()
	if r.invalid.Load() {
		return nil, eventadapter.ErrReaderInvalidated
	}
	if !r.busy.CompareAndSwap(false, true) {
		return nil, eventadapter.ErrInvalidState
	}
	readCtx, cancel := mergeContext(ctx, r.stop)
	defer cancel()
	message, err := r.native.FetchMessage(readCtx)
	if err != nil {
		r.busy.Store(false)
		r.invalid.Store(true)
		return nil, err
	}
	metadata := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		metadata[header.Key] = string(header.Value)
	}
	return &delivery{owner: r, native: message, message: eventadapter.Message{EventName: r.topic,
		Payload: slices.Clone(message.Value), OccurredAt: message.Time, Metadata: metadata}}, nil
}

func (r *reader) Close(ctx context.Context) error { return r.life.close(ctx) }

type delivery struct {
	owner   *reader
	native  kafka.Message
	message eventadapter.Message
	settled atomic.Bool
}

func (d *delivery) Message() eventadapter.Message {
	message := d.message
	message.Payload = slices.Clone(message.Payload)
	message.Metadata = maps.Clone(message.Metadata)
	return message
}

func (d *delivery) Ack(ctx context.Context) error  { return d.settle(ctx, true) }
func (d *delivery) Nack(ctx context.Context) error { return d.settle(ctx, false) }

func (d *delivery) settle(ctx context.Context, success bool) error {
	if ctx == nil {
		return checkContext(ctx)
	}
	if err := d.owner.life.enter(); err != nil {
		return err
	}
	defer d.owner.life.leave()
	if !d.settled.CompareAndSwap(false, true) {
		return eventadapter.ErrInvalidState
	}
	defer d.owner.busy.Store(false)
	if err := ctx.Err(); err != nil {
		d.owner.invalid.Store(true)
		return err
	}
	if !success {
		d.owner.invalid.Store(true)
		return fmt.Errorf("Kafka failed offset must be redelivered: %w", eventadapter.ErrReaderInvalidated)
	}
	if err := d.owner.native.CommitMessages(ctx, d.native); err != nil {
		d.owner.invalid.Store(true)
		return err
	}
	return nil
}

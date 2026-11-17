English | [Korean](README.ko.md)

# Spine Kafka Platform

A Go module connecting Kafka to Spine's public event contract, `spine.event-adapter/v1`. It implements Factory, Reader, Delivery, and Publisher from `adapter/event`, without importing domain events, DI, or kernel internals.

## Preview installation

Use this channel to try features under development and check compatibility. The published tag is [`v0.2.0-preview.3`](https://github.com/NARUBROWN/spine-kafka-platform/tree/v0.2.0-preview.3). Its `go.mod` specifies Spine `v0.7.0-preview.4` and Go 1.25.5 or later. The release policy on this branch targets `v0.2.0`.

```sh
go get github.com/NARUBROWN/spine-kafka-platform@v0.2.0-preview.3
```

Other channels: [RC](https://github.com/NARUBROWN/spine-kafka-platform/tree/rc) | [Stable](https://github.com/NARUBROWN/spine-kafka-platform/tree/main).

Register the factory with `spine.WithEventAdapter(name, factory)`. The platform owns broker connections; the core owns consumer execution and reader recreation. The example below configures a worker without HTTP. HTTP applications also pass an Echo platform option to `spine.New`. It uses the public event contract and App integration API from Spine v0.6.5.

```go
import (
    "github.com/NARUBROWN/spine"
    kafkaplatform "github.com/NARUBROWN/spine-kafka-platform"
)

type orderDestinations struct{}

func (orderDestinations) PublishDestinations() []string {
    return []string{"orders.created"}
}

func buildWorker() (spine.App, error) {
    factory, err := kafkaplatform.NewFactory(kafkaplatform.Config{Brokers: []string{"kafka.example.com:9093"}, Consume: true, Publish: true, GroupID: "orders", PublishDestinations: orderDestinations{}})
    if err != nil { return nil, err }
    return spine.New(spine.WithEventAdapter("kafka", factory)), nil
}
```

Register consumer constructors and handlers with `app.Consumers().Register`, then run `app.Run(boot.Options{})`. Configure common policies with `spine.WithEventConsumerRetry` and `spine.WithEventSettlementTimeout`. Prepare retains readers, and Run starts consumption through the same preparation path. See the [App migration guide](https://github.com/NARUBROWN/spine/blob/5d8ea93/docs/migration/event-platforms.md).

## Usage

```go
import (
    "context"

    eventadapter "github.com/NARUBROWN/spine/contract/event"
    kafkaplatform "github.com/NARUBROWN/spine-kafka-platform"
)

func consumeOne(ctx context.Context) error {
    factory, err := kafkaplatform.NewFactory(kafkaplatform.Config{
        Brokers: []string{"localhost:9092"},
        Consume: true,
        GroupID: "order-worker",
        AllowInsecureTransport: true, // Use only with an isolated local development broker.
    })
    if err != nil { return err }
    reader, err := factory.NewReader(ctx, eventadapter.Subscription{Topic: "order.created"})
    if err != nil { return err }
    defer reader.Close(context.Background())
    delivery, err := reader.Read(ctx)
    if err != nil { return err }
    // Process delivery.Message(), then call Ack on success or Nack on failure.
    return delivery.Ack(ctx)
}
```

Publishing in this source requires `Config.Publish: true` and `Config.PublishDestinations`. `NewFactory` validates the destination list, and `NewPublisher` checks metadata for the registered Kafka topics. Pass an already serialized body to `Publish(ctx, eventadapter.Message{EventName: ..., Payload: ...})`. A nil body is allowed and is not serialized to JSON again.

## Configuration

| Field | Behavior |
| --- | --- |
| `Brokers` | Required, nonempty list of broker addresses |
| `Consume`, `Publish` | Enable reading and publishing independently; enable at least one |
| `GroupID` | Required when consuming |
| `PublishDestinations` | Required when publishing; provides logical names through `PublishDestinations() []string` |
| `TopicPrefix` | Adds the same prefix to published event names and logical subscription keys |
| `TLS` | Shared TLS configuration; defaults to TLS 1.2 or later |
| `Dialer`, `Transport` | Advanced kafka-go settings for reading and publishing. TLS settings and configuration slices are copied. Callers must safely share callbacks, SASL, and resolvers. |
| `AllowInsecureTransport` | Defaults to false. Explicitly permits local plaintext connections; cannot be combined with TLS settings. |
| `StartupTimeout` | Reader metadata readiness timeout; defaults to 10 seconds |
| `WriteTimeout` | Publishing client read/write timeout; defaults to 10 seconds |

Factory creation and Capabilities queries do not access the network. NewReader requests Kafka topic metadata before returning; joining the consumer group and receiving the first message happen in Read. NewPublisher prepares transport resources; connections and destination checks are performed in Publish. `Transport.Context` is set to a background context so the platform owns resource lifetime.

## Processing and resource lifetime

- Each Reader has one unsettled delivery. Ack commits offsets synchronously. Nack returns `eventadapter.ErrReaderInvalidated`; close and recreate the Reader to prevent reading or committing offsets beyond the failed message. An ACK error also prevents further reads on that Reader.
- `Subscription.Topic` selects the kernel handler. Received Message.EventName contains the logical subscription key. Metadata contains Kafka headers as strings; the last value wins for duplicate header keys.
- Publishing uses `RequiredAcks: RequireAll`. Retrying after a failed acknowledgement or cancellation can create duplicates. It does not guarantee consumer completion or exactly-once processing.
- Call Read and delivery settlement serially. Publish supports concurrent calls. Payloads and metadata are copied, preserving binary bodies.
- Close rejects new work, waits for in-flight settlement and publishing, and cleans up resources once. Pending Reader reads are cancelled. The Close context only limits waiting; later calls can wait for the same cleanup result.
- Cancelling the creation context after success does not destroy the returned resource. Creation failures clean up resources and return nil.

## Verification

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_KAFKA_BROKER=localhost:9092 go test -race . -run '^TestLive' -count=1 -timeout=2m
```

Live tests are skipped when broker environment variables are absent. They check binary bodies, header round trips, redelivery after Nack, and commit ordering for subsequent messages. CI runs unit, race, and Redpanda live checks separately. Production TLS/SASL requires separate validation.

License: [MIT](LICENSE). Contract: [Spine adapter/event](https://github.com/NARUBROWN/spine/tree/1889d66/adapter/event).

## v0.1.0 compatibility

This release requires Spine `v0.6.5` or later, which includes the public contracts. Install the official tag without a `replace` pointing to a private path.

```sh
go get github.com/NARUBROWN/spine-kafka-platform@v0.1.0
```

## Releases and public contracts

This platform publishes its own versions and release channels. The Spine version in `go.mod` is the minimum dependency for its public contracts and is managed separately from the core release channel. CI and public installation checks verify the platform against this module's dependencies. Core releases verify combinations of a new core and existing platform tags.

The next publication target is `v0.3.0`.

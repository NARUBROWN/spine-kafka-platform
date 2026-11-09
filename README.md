# Spine Kafka Platform

Kafka를 Spine의 공개 이벤트 계약 `spine.event-adapter/v1`에 연결하는 Go 모듈입니다. `adapter/event`의 Factory, Reader, Delivery와 Publisher를 구현하며 도메인 이벤트, DI나 커널 내부 패키지를 import하지 않습니다.

## Stable 설치

일반 개발과 운영에 사용하는 정식 릴리스 채널입니다. 공개 태그는 [`v0.2.0`](https://github.com/NARUBROWN/spine-kafka-platform/tree/v0.2.0)입니다. 이 태그의 `go.mod`는 Spine `v0.7.0-preview.4`와 Go 1.25.5 이상을 지정합니다.

```sh
go get github.com/NARUBROWN/spine-kafka-platform@v0.2.0
```

다른 채널은 [Preview](https://github.com/NARUBROWN/spine-kafka-platform/tree/preview) | [RC](https://github.com/NARUBROWN/spine-kafka-platform/tree/rc)에서 확인하세요.


App에는 `spine.WithEventAdapter(name, factory)`로 등록합니다. 브로커 연결은 이 플랫폼이, 소비 실행과 reader 재연결은 코어가 맡습니다. 아래는 HTTP 없는 워커 구성입니다. HTTP 앱은 `spine.New`에 Echo 플랫폼 옵션도 함께 넘깁니다. Spine v0.6.5의 공개 이벤트 계약과 App 연결 API를 사용합니다.

```go
import (
    "github.com/NARUBROWN/spine"
    kafkaplatform "github.com/NARUBROWN/spine-kafka-platform"
)

func buildWorker() (spine.App, error) {
    factory, err := kafkaplatform.NewFactory(kafkaplatform.Config{Brokers: []string{"kafka.example.com:9093"}, Consume: true, Publish: true, GroupID: "orders"})
    if err != nil { return nil, err }
    return spine.New(spine.WithEventAdapter("kafka", factory)), nil
}
```

기존 consumer 생성자와 `app.Consumers().Register`를 등록한 뒤 `app.Run(boot.Options{})`로 실행합니다. 공통 정책은 `spine.WithEventConsumerRetry`와 `spine.WithEventSettlementTimeout`으로 조정합니다. Prepare는 reader를 보관하고 Run이 같은 준비 경로에서 소비를 시작합니다. [App 전환 안내](https://github.com/NARUBROWN/spine/blob/5d8ea93/docs/migration/event-platforms.md)를 참고하세요.

## 사용

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
        AllowInsecureTransport: true, // 격리된 로컬 개발 브로커에서만 사용합니다.
    })
    if err != nil { return err }
    reader, err := factory.NewReader(ctx, eventadapter.Subscription{Topic: "order.created"})
    if err != nil { return err }
    defer reader.Close(context.Background())
    delivery, err := reader.Read(ctx)
    if err != nil { return err }
    // delivery.Message()의 본문을 처리한 뒤 성공하면 Ack, 실패하면 Nack을 호출합니다.
    return delivery.Ack(ctx)
}
```

발행은 `Config.Publish: true`와 `factory.NewPublisher(ctx)`를 사용합니다. `Publish(ctx, eventadapter.Message{EventName: ..., Payload: ...})`에는 이미 직렬화한 본문을 넘깁니다. nil 본문도 허용하며 다시 JSON으로 변환하지 않습니다.

## 설정

| 필드 | 동작 |
| --- | --- |
| `Brokers` | 비어 있지 않은 브로커 주소 목록. 필수입니다. |
| `Consume`, `Publish` | 읽기, 발행 역할을 각각 활성화합니다. 하나 이상 필요합니다. |
| `GroupID` | 소비를 활성화하면 필수입니다. |
| `TopicPrefix` | 발행 이벤트 이름과 논리 구독 키 앞에 같은 접두사를 붙입니다. |
| `TLS` | 공통 TLS 설정. 기본값은 TLS 1.2 이상입니다. |
| `Dialer`, `Transport` | kafka-go의 읽기, 발행 고급 설정. TLS와 설정 slice를 복사합니다. 사용자 콜백, SASL, resolver는 호출자가 안전하게 공유해야 합니다. |
| `AllowInsecureTransport` | 기본 false. 로컬 평문 연결을 명시적으로 허용하며 TLS 설정과 함께 사용할 수 없습니다. |
| `StartupTimeout` | Reader metadata 준비 제한 시간. 기본 10초입니다. |
| `WriteTimeout` | 발행 클라이언트 읽기, 쓰기 제한 시간. 기본 10초입니다. |

factory 생성과 Capabilities 조회는 네트워크를 사용하지 않습니다. NewReader는 실제 Kafka topic metadata를 요청한 뒤 반환하며, consumer group 참여와 첫 메시지 수신은 Read에서 진행합니다. NewPublisher는 전송 자원을 준비하고 실제 연결, 대상 확인은 Publish에서 수행합니다. `Transport.Context`는 플랫폼이 자원 수명을 소유하도록 background context로 설정합니다.

## 처리와 수명

- 한 Reader에서 미확인 전달은 하나입니다. Ack는 동기 offset commit이며 Nack은 `eventadapter.ErrReaderInvalidated`를 반환합니다. 실패 offset보다 뒤의 offset을 읽거나 커밋하지 않도록 Reader를 닫고 다시 생성해야 합니다. ACK 오류도 같은 Reader를 더 읽지 못하게 합니다.
- `Subscription.Topic`은 커널의 핸들러 선택 키입니다. 수신 Message.EventName에는 논리 구독 키를 넣고 Metadata에는 Kafka headers를 문자열로 담습니다. 같은 header key가 반복되면 마지막 값을 사용합니다.
- 발행은 `RequiredAcks: RequireAll`을 사용합니다. 확인 실패, 취소 뒤 재발행은 중복을 만들 수 있습니다. 소비 완료나 exactly-once를 보장하지 않습니다.
- Read와 전달 확인은 직렬 호출합니다. Publish는 동시에 호출할 수 있습니다. payload와 메타데이터는 복사하며 binary 본문을 보존합니다.
- Close는 새 작업을 막고 진행 중인 확인, 발행이 끝난 뒤 자원을 한 번 정리합니다. Reader의 대기 중 Read는 취소됩니다. Close context는 완료 대기만 제한하며 다시 호출해 같은 정리 결과를 기다릴 수 있습니다.
- 생성 context를 성공 후 취소해도 반환된 자원은 유지됩니다. 생성 실패는 자원을 정리하고 nil 결과를 반환합니다.

## 검증

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_KAFKA_BROKER=localhost:9092 go test -race . -run '^TestLive' -count=1 -timeout=2m
```

브로커 환경변수가 없으면 live 테스트는 건너뜁니다. Live 테스트는 binary 본문, header 왕복과 Nack 후 실패 메시지의 재전달, 이어지는 메시지의 commit 순서를 검사합니다. CI는 단위, race 검사와 Redpanda 기반 live 검사를 분리해서 실행합니다. TLS/SASL 운영 환경은 별도로 검증해야 합니다.

라이선스: [MIT](LICENSE). 계약: [Spine adapter/event](https://github.com/NARUBROWN/spine/tree/1889d66/adapter/event).

## v0.1.0 호환 버전

이 릴리스는 공개 계약을 포함한 Spine `v0.6.5` 이상을 요구합니다. 개인 경로의 `replace` 없이 정식 태그로 설치합니다.

```sh
go get github.com/NARUBROWN/spine-kafka-platform@v0.1.0
```

## 릴리스와 공개 계약

이 플랫폼은 자체 버전과 채널로 발행합니다. `go.mod`의 Spine 버전은 사용하는 공개 계약의 최소 의존성이며 코어 릴리스 채널과 별도로 관리합니다. CI와 공개 설치 검증은 이 모듈의 의존성으로 플랫폼 자체를 검사합니다. 새로운 코어와 기존 플랫폼 태그의 조합 검증은 코어 릴리스에서 수행합니다.

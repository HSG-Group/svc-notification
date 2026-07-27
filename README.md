# notification-svc

The **Notification Service** is the bounded context responsible for delivering **real-time and async notifications** across three channels — **push** (FCM / APNs), **email** (SendGrid), and **in-app** (WebSocket via Redis pub/sub) — to Harmony users. It is written in **Go 1.22** and follows **DDD + Clean Architecture** (Ports & Adapters), driven entirely by Kafka events from other services.

---

## Table of contents

- [Overview](#overview)
- [Bounded context](#bounded-context)
- [Architecture](#architecture)
- [Folder structure](#folder-structure)
- [Domain layer](#domain-layer)
- [Application layer](#application-layer)
- [Infrastructure layer](#infrastructure-layer)
- [Delivery channels](#delivery-channels)
- [Retry strategy](#retry-strategy)
- [Kafka events consumed](#kafka-events-consumed)
- [Domain events published](#domain-events-published)
- [Database](#database)
- [API reference](#api-reference)
- [Local development (localhost setup)](#local-development-localhost-setup)
- [Testing strategy](#testing-strategy)
- [Environment variables](#environment-variables)
- [Related services](#related-services)

---

## Overview

| Property | Value |
|---|---|
| Language | Go 1.22+ |
| Architecture | DDD + Clean Architecture (Ports & Adapters) |
| Trigger mechanism | Kafka consumers (event-driven — no REST ingress) |
| Delivery channels | Push (FCM / APNs) · Email (SendGrid) · In-app (Redis pub/sub) |
| Database | PostgreSQL (notification history, preferences, retry queue) |
| Cache | Redis (preference cache TTL 5 min · in-app pub/sub) |
| HTTP port | `8085` (preference API + health only) |
| Kafka group | `notification-svc` |

---

## Bounded context

`notification-svc` is a **pure consumer** — it does not expose a public API for other services to call directly. It reacts to domain events from upstream services, decides which channels to use (based on user preferences), and delivers the notification.

```
auth-svc         ──► identity.user.registered      ──► welcome email
auth-svc         ──► identity.user.loggedin         ──► suspicious login push
user-svc         ──► user.friend.request            ──► push + in-app
user-svc         ──► user.friend.accepted           ──► push + in-app
community-svc    ──► community.server.created       ──► in-app (server ready)
community-svc    ──► community.member.joined        ──► in-app to server members
community-svc    ──► community.role.assigned        ──► in-app to target user
```

> **Rule:** notification-svc never initiates a domain event of its own that other services depend on. It is a leaf node in the event graph.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Infrastructure layer                       │
│                                                                  │
│  Inbound                          Outbound adapters              │
│  ─────────                        ─────────────────              │
│  KafkaConsumer (events)           FcmAdapter (push)              │
│  HTTP (preference API)            ApnsAdapter (push)             │
│                                   SendGridAdapter (email)        │
│                                   RedisPubSubAdapter (in-app)    │
│                                   PgNotificationRepo             │
│                                   PgPreferenceRepo               │
│                                   RetryWorker (BackgroundSvc)    │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                   Application layer                      │   │
│   │                                                          │   │
│   │  Commands                    Ports (interfaces)          │   │
│   │  ──────────                  ────────────────            │   │
│   │  DispatchNotification        IPushProvider               │   │
│   │  UpdatePreference            IEmailProvider              │   │
│   │  ProcessRetry                IInAppProvider              │   │
│   │                              INotificationRepo           │   │
│   │  Services                    IPreferenceRepo             │   │
│   │  ──────────                  ICacheService               │   │
│   │  PreferenceFilter                                        │   │
│   │  ChannelDispatcher                                       │   │
│   │  NotificationRouter                                      │   │
│   │                                                          │   │
│   │   ┌─────────────────────────────────────────────────┐   │   │
│   │   │               Domain layer                       │   │   │
│   │   │  Notification  · NotificationChannel enum        │   │   │
│   │   │  UserPreference · RetryRecord                    │   │   │
│   │   │  DeviceToken   · value objects · domain events   │   │   │
│   │   └─────────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Folder structure

```
notification-svc/
├── domain/
│   ├── notification/
│   │   ├── notification.go           ← Notification aggregate root
│   │   ├── notification_channel.go   ← Enum: Push, Email, InApp
│   │   └── notification_status.go   ← Enum: Pending, Sent, Failed, Dead
│   ├── preference/
│   │   ├── user_preference.go        ← Aggregate: push/email/in-app bools
│   │   └── preference_repository.go  ← IPreferenceRepo interface (port)
│   ├── device/
│   │   ├── device_token.go           ← Entity: userId, token, platform
│   │   └── device_repository.go      ← IDeviceRepo interface (port)
│   ├── retry/
│   │   └── retry_record.go           ← Entity: attempts, nextRetryAt, backoff
│   └── valueobjects/
│       ├── notification_type.go      ← FriendRequest, ServerCreated, LoginAlert…
│       └── recipient.go             ← value object: userId + resolved channels
│
├── application/
│   ├── command/
│   │   ├── dispatch_notification.go  ← Main use case: route + filter + dispatch
│   │   ├── update_preference.go      ← Update user opt-in/out settings
│   │   └── process_retry.go         ← Retry worker use case
│   ├── service/
│   │   ├── notification_router.go    ← Maps event type → NotificationDto + channels
│   │   ├── preference_filter.go     ← Removes channels user has opted out of
│   │   └── channel_dispatcher.go    ← Fan-out to enabled channels concurrently
│   ├── port/
│   │   ├── push_provider.go         ← IPushProvider interface
│   │   ├── email_provider.go        ← IEmailProvider interface
│   │   ├── inapp_provider.go        ← IInAppProvider interface
│   │   ├── notification_repo.go     ← INotificationRepo interface
│   │   ├── preference_repo.go       ← IPreferenceRepo interface
│   │   ├── device_repo.go           ← IDeviceRepo interface
│   │   └── cache_port.go            ← ICacheService interface
│   └── dto/
│       ├── notification_dto.go       ← Type, Title, Body, Data, TargetUserId
│       ├── dispatch_result.go        ← Per-channel delivery results
│       └── event_payload.go         ← Kafka event payloads (one per topic)
│
├── infrastructure/
│   ├── messaging/
│   │   ├── kafka_consumer.go         ← @KafkaListener equivalent — fan-out per topic
│   │   ├── event_handlers/
│   │   │   ├── friend_request_handler.go
│   │   │   ├── friend_accepted_handler.go
│   │   │   ├── user_registered_handler.go
│   │   │   ├── login_alert_handler.go
│   │   │   ├── server_created_handler.go
│   │   │   └── role_assigned_handler.go
│   │   └── kafka_config.go
│   ├── push/
│   │   ├── fcm_adapter.go            ← implements IPushProvider (Android + Web)
│   │   └── apns_adapter.go          ← implements IPushProvider (iOS)
│   ├── email/
│   │   ├── sendgrid_adapter.go       ← implements IEmailProvider
│   │   ├── templates/
│   │   │   ├── welcome.html
│   │   │   ├── friend_request.html
│   │   │   └── login_alert.html
│   │   └── template_renderer.go     ← Go text/template rendering
│   ├── inapp/
│   │   └── redis_pubsub_adapter.go  ← implements IInAppProvider
│   ├── persistence/
│   │   ├── pg_notification_repo.go   ← implements INotificationRepo
│   │   ├── pg_preference_repo.go    ← implements IPreferenceRepo
│   │   ├── pg_device_repo.go        ← implements IDeviceRepo
│   │   └── pg_retry_repo.go
│   ├── cache/
│   │   └── redis_cache_adapter.go   ← implements ICacheService
│   ├── http/
│   │   ├── preference_handler.go    ← GET/PUT /users/:id/preferences
│   │   ├── device_handler.go        ← POST/DELETE /users/:id/devices
│   │   └── router.go
│   └── worker/
│       └── retry_worker.go          ← Background goroutine — polls retry_queue
│
├── cmd/
│   └── main.go                       ← DI wiring, start Kafka consumer + HTTP server
│
├── migrations/
│   ├── 001_create_notifications.sql
│   ├── 002_create_user_preferences.sql
│   ├── 003_create_device_tokens.sql
│   └── 004_create_retry_queue.sql
│
├── Dockerfile
├── docker-compose.yml                ← Local dev: PostgreSQL + Kafka + Redis
├── .env.example
├── go.mod
└── go.sum
```

---

## Domain layer

### `Notification` aggregate

```go
type Notification struct {
    id           uuid.UUID
    userId       uuid.UUID
    notifType    NotificationType     // FriendRequest, LoginAlert, ServerCreated…
    title        string
    body         string
    data         map[string]string   // deep-link payload for push
    channels     []NotificationChannel
    status       NotificationStatus  // Pending, Sent, PartialFailed, Dead
    deliveries   []DeliveryRecord    // per-channel result
    createdAt    time.Time
}

func Create(userId uuid.UUID, t NotificationType, title, body string,
            channels []NotificationChannel) *Notification
func (n *Notification) RecordDelivery(channel NotificationChannel, success bool, err string)
func (n *Notification) IsFullyDelivered() bool
func (n *Notification) NeedsRetry() bool
```

### `UserPreference` aggregate

```go
type UserPreference struct {
    userId    uuid.UUID
    pushEnabled    bool
    emailEnabled   bool
    inAppEnabled   bool
    // per-type overrides: e.g. loginAlert → email only
    typeOverrides  map[NotificationType][]NotificationChannel
    updatedAt  time.Time
}

func (p *UserPreference) EnabledChannelsFor(notifType NotificationType) []NotificationChannel
func (p *UserPreference) SetPush(enabled bool)
func (p *UserPreference) SetEmail(enabled bool)
func (p *UserPreference) SetInApp(enabled bool)
```

### `DeviceToken` entity

```go
type DeviceToken struct {
    id        uuid.UUID
    userId    uuid.UUID
    token     string
    platform  Platform   // Android, iOS, Web
    createdAt time.Time
    lastUsed  time.Time
}
```

### `NotificationType` value object

```go
type NotificationType string

const (
    FriendRequest   NotificationType = "friend_request"
    FriendAccepted  NotificationType = "friend_accepted"
    ServerCreated   NotificationType = "server_created"
    MemberJoined    NotificationType = "member_joined"
    RoleAssigned    NotificationType = "role_assigned"
    LoginAlert      NotificationType = "login_alert"
    WelcomeEmail    NotificationType = "welcome_email"
)
```

---

## Application layer

### `DispatchNotification` use case

This is the main use case — called by every Kafka event handler:

```go
func (uc *DispatchNotificationUseCase) Execute(ctx context.Context, dto NotificationDto) error {
    // 1. Load user preferences (Redis cache → PostgreSQL fallback)
    prefs, err := uc.prefRepo.FindByUserId(ctx, dto.TargetUserId)

    // 2. Filter channels based on user opt-in
    enabledChannels := uc.prefFilter.Filter(prefs, dto.Type)
    if len(enabledChannels) == 0 {
        return nil // user opted out of all channels for this type
    }

    // 3. Create Notification aggregate
    notif := domain.Create(dto.TargetUserId, dto.Type, dto.Title, dto.Body, enabledChannels)

    // 4. Persist as Pending
    uc.notifRepo.Save(ctx, notif)

    // 5. Dispatch to channels concurrently
    results := uc.dispatcher.Dispatch(ctx, notif, dto.Data)

    // 6. Record delivery results
    for _, r := range results {
        notif.RecordDelivery(r.Channel, r.Success, r.Error)
    }

    // 7. Queue failed channels for retry
    for _, r := range results {
        if !r.Success {
            uc.retryRepo.Enqueue(ctx, RetryRecord{
                NotificationId: notif.id,
                Channel:        r.Channel,
                Attempts:       0,
                NextRetryAt:    time.Now().Add(1 * time.Second),
            })
        }
    }

    return uc.notifRepo.Save(ctx, notif)
}
```

### `ChannelDispatcher` application service

Fans out to all enabled channels concurrently using goroutines:

```go
func (d *ChannelDispatcher) Dispatch(ctx context.Context,
    notif *Notification, data map[string]string) []DispatchResult {

    var wg sync.WaitGroup
    results := make([]DispatchResult, len(notif.channels))

    for i, ch := range notif.channels {
        wg.Add(1)
        go func(idx int, channel NotificationChannel) {
            defer wg.Done()
            var err error
            switch channel {
            case Push:
                err = d.pushProvider.Send(ctx, notif, data)
            case Email:
                err = d.emailProvider.Send(ctx, notif)
            case InApp:
                err = d.inAppProvider.Publish(ctx, notif)
            }
            results[idx] = DispatchResult{Channel: channel, Success: err == nil}
        }(i, ch)
    }
    wg.Wait()
    return results
}
```

### Port interfaces

```go
// port/push_provider.go
type IPushProvider interface {
    Send(ctx context.Context, notif *Notification, data map[string]string) error
    Platform() Platform   // Android, iOS, Web
}

// port/email_provider.go
type IEmailProvider interface {
    Send(ctx context.Context, notif *Notification) error
}

// port/inapp_provider.go
type IInAppProvider interface {
    Publish(ctx context.Context, notif *Notification) error
}

// port/notification_repo.go
type INotificationRepo interface {
    Save(ctx context.Context, notif *Notification) error
    FindById(ctx context.Context, id uuid.UUID) (*Notification, error)
    FindByUserId(ctx context.Context, userId uuid.UUID, limit int) ([]*Notification, error)
    MarkRead(ctx context.Context, id uuid.UUID) error
}

// port/preference_repo.go
type IPreferenceRepo interface {
    FindByUserId(ctx context.Context, userId uuid.UUID) (*UserPreference, error)
    Save(ctx context.Context, pref *UserPreference) error
}
```

---

## Infrastructure layer

### Kafka consumer (`infrastructure/messaging/kafka_consumer.go`)

Each topic maps to a dedicated handler. Handlers call `DispatchNotificationUseCase`:

```go
func (c *KafkaConsumer) Start(ctx context.Context) {
    topics := []string{
        "identity.user.registered",
        "identity.user.loggedin",
        "user.friend.request",
        "user.friend.accepted",
        "community.server.created",
        "community.member.joined",
        "community.role.assigned",
    }
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: c.brokers,
        GroupID: "notification-svc",
        Topics:  topics,
    })
    for {
        msg, _ := reader.FetchMessage(ctx)
        go c.handle(ctx, msg)   // each message handled concurrently
        reader.CommitMessages(ctx, msg)
    }
}
```

### FCM adapter (`infrastructure/push/fcm_adapter.go`)

```go
func (a *FcmAdapter) Send(ctx context.Context,
    notif *Notification, data map[string]string) error {

    tokens, _ := a.deviceRepo.FindByUserId(ctx, notif.userId, Android)
    if len(tokens) == 0 {
        return nil // no Android devices registered
    }

    for _, token := range tokens {
        msg := &messaging.Message{
            Token: token.token,
            Notification: &messaging.Notification{
                Title: notif.title,
                Body:  notif.body,
            },
            Data:    data,
            Android: &messaging.AndroidConfig{Priority: "high"},
        }
        _, err := a.fcmClient.Send(ctx, msg)
        if messaging.IsRegistrationTokenNotRegistered(err) {
            a.deviceRepo.Delete(ctx, token.id)   // stale token — remove
            continue
        }
    }
    return nil
}
```

### SendGrid adapter (`infrastructure/email/sendgrid_adapter.go`)

```go
func (a *SendGridAdapter) Send(ctx context.Context, notif *Notification) error {
    // 1. Resolve user email via gRPC call to user-svc
    profile, _ := a.userClient.GetProfile(ctx, notif.userId.String())

    // 2. Render HTML template
    html, _ := a.renderer.Render(string(notif.notifType), map[string]any{
        "Title": notif.title,
        "Body":  notif.body,
    })

    // 3. Call SendGrid API
    message := mail.NewSingleEmail(
        mail.NewEmail("Harmony", "noreply@harmony.app"),
        notif.title,
        mail.NewEmail("", profile.Email),
        notif.body,  // plain text fallback
        html,
    )
    _, err := a.client.Send(message)
    return err
}
```

### Redis in-app adapter (`infrastructure/inapp/redis_pubsub_adapter.go`)

```go
func (a *RedisPubSubAdapter) Publish(ctx context.Context, notif *Notification) error {
    payload, _ := json.Marshal(map[string]any{
        "type":    notif.notifType,
        "title":   notif.title,
        "body":    notif.body,
        "id":      notif.id,
        "read":    false,
        "created": notif.createdAt,
    })
    // ws-gateway subscribes to "inapp:{userId}" and fans out to active WebSocket connections
    return a.redis.Publish(ctx, fmt.Sprintf("inapp:%s", notif.userId), payload).Err()
}
```

### Retry worker (`infrastructure/worker/retry_worker.go`)

```go
func (w *RetryWorker) Start(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    for {
        select {
        case <-ticker.C:
            w.processBatch(ctx)
        case <-ctx.Done():
            return
        }
    }
}

func (w *RetryWorker) processBatch(ctx context.Context) {
    due := w.retryRepo.FindDue(ctx, time.Now(), 50)
    for _, record := range due {
        err := w.dispatcher.DispatchSingle(ctx, record.NotificationId, record.Channel)
        if err != nil {
            if record.Attempts >= 5 {
                w.retryRepo.MoveToDLQ(ctx, record.id)
            } else {
                backoff := time.Duration(math.Pow(2, float64(record.Attempts))) * time.Second
                jitter  := time.Duration(rand.Intn(500)) * time.Millisecond
                w.retryRepo.Reschedule(ctx, record.id, time.Now().Add(backoff+jitter))
            }
        } else {
            w.retryRepo.MarkDone(ctx, record.id)
        }
    }
}
```

---

## Delivery channels

### Push — FCM (Android) + APNs (iOS)

| Property | Value |
|---|---|
| Android | Firebase Cloud Messaging (FCM HTTP v1 API) |
| iOS | Apple Push Notification service (APNs HTTP/2) |
| Web | FCM Web Push |
| Authentication | FCM: Service account JSON · APNs: `.p8` key + Team ID + Key ID |
| Priority | `high` (Android) / `time-sensitive` (iOS) for real-time alerts |
| Stale token handling | FCM `UNREGISTERED` error → delete token from DB |

### Email — SendGrid

| Property | Value |
|---|---|
| Provider | SendGrid transactional email API v3 |
| Authentication | API key (env var `SENDGRID_API_KEY`) |
| Templates | Go `text/html/template` (stored in `infrastructure/email/templates/`) |
| From address | `noreply@harmony.app` |
| Unsubscribe | One-click unsubscribe link in every email footer |
| Rate limit | SendGrid plan dependent; notification-svc respects `429` with backoff |

### In-app — Redis pub/sub → WebSocket

| Property | Value |
|---|---|
| Mechanism | `PUBLISH inapp:{userId} {payload}` |
| Consumer | `ws-gateway` subscribes to these channels and fans out to open WebSocket connections |
| Persistence | Notification written to DB — client fetches history on reconnect |
| Read receipts | `PUT /users/:id/notifications/:notifId/read` marks as read |

---

## Retry strategy

Failed deliveries (network errors, `5xx` responses) are queued in `retry_queue` and retried with **exponential backoff + jitter**:

| Attempt | Delay | Formula |
|---|---|---|
| 1 | ~1s | `2⁰ × 1s + rand(0–500ms)` |
| 2 | ~2s | `2¹ × 1s + rand(0–500ms)` |
| 3 | ~4s | `2² × 1s + rand(0–500ms)` |
| 4 | ~8s | `2³ × 1s + rand(0–500ms)` |
| 5 | ~16s | `2⁴ × 1s + rand(0–500ms)` |
| > 5 | — | Moved to `dead_letter_queue` — alert fired |

Jitter prevents a thundering herd when a push provider recovers. The retry worker polls every 10 seconds for due records.

**Dead letter queue (DLQ):** Notifications that exceed 5 attempts land in `dead_letter_queue`. A Prometheus alert fires when DLQ row count exceeds 100. These are reviewed manually and can be replayed via the admin API.

---

## Kafka events consumed

| Topic | Handler | Action |
|---|---|---|
| `identity.user.registered` | `UserRegisteredHandler` | Send welcome email |
| `identity.user.loggedin` | `LoginAlertHandler` | Push + email if new device / new country |
| `user.friend.request` | `FriendRequestHandler` | Push + in-app to addressee |
| `user.friend.accepted` | `FriendAcceptedHandler` | Push + in-app to requester |
| `community.server.created` | `ServerCreatedHandler` | In-app to owner |
| `community.member.joined` | `MemberJoinedHandler` | In-app to server members (batched) |
| `community.role.assigned` | `RoleAssignedHandler` | In-app to target user |

---

## Domain events published

`notification-svc` is a leaf node — it publishes no Kafka events that other services depend on.

It does write all notifications to `notifications` table for the client to poll via the REST API.

---

## Database

### Schema

```sql
-- notifications
CREATE TABLE notifications (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL,
    type        VARCHAR(64) NOT NULL,
    title       TEXT NOT NULL,
    body        TEXT NOT NULL,
    data        JSONB,
    channels    TEXT[] NOT NULL,          -- ['push','email','in_app']
    status      VARCHAR(32) NOT NULL DEFAULT 'pending',
    read        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at     TIMESTAMPTZ
);
CREATE INDEX idx_notifications_user ON notifications (user_id, created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications (user_id, read)
    WHERE read = FALSE;

-- delivery_records
CREATE TABLE delivery_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    channel         VARCHAR(16) NOT NULL,
    success         BOOLEAN NOT NULL,
    error           TEXT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- user_preferences
CREATE TABLE user_preferences (
    user_id         UUID PRIMARY KEY,
    push_enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    email_enabled   BOOLEAN NOT NULL DEFAULT TRUE,
    in_app_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    type_overrides  JSONB,               -- per-type channel overrides
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- device_tokens
CREATE TABLE device_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL,
    token       TEXT NOT NULL UNIQUE,
    platform    VARCHAR(16) NOT NULL,    -- android, ios, web
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_device_tokens_user ON device_tokens (user_id, platform);

-- retry_queue
CREATE TABLE retry_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL REFERENCES notifications(id),
    channel         VARCHAR(16) NOT NULL,
    attempts        SMALLINT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_retry_queue_due ON retry_queue (next_retry_at)
    WHERE next_retry_at <= now();

-- dead_letter_queue
CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL,
    channel         VARCHAR(16) NOT NULL,
    last_error      TEXT,
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Redis key patterns

| Key | Value | TTL | Purpose |
|---|---|---|---|
| `notif-pref:{userId}` | JSON `UserPreference` | 5 min | Preference cache |
| `inapp:{userId}` | pub/sub channel | — | In-app delivery to ws-gateway |

---

## API reference

The HTTP API is minimal — notification-svc is primarily event-driven. The REST endpoints serve client preference management and notification history.

| Method | Path | Description | Auth |
|---|---|---|---|
| `GET` | `/api/v1/users/:id/notifications` | List notifications (paginated, newest first) | ✅ JWT |
| `PUT` | `/api/v1/users/:id/notifications/:notifId/read` | Mark notification as read | ✅ own only |
| `PUT` | `/api/v1/users/:id/notifications/read-all` | Mark all as read | ✅ own only |
| `GET` | `/api/v1/users/:id/preferences` | Get notification preferences | ✅ own only |
| `PUT` | `/api/v1/users/:id/preferences` | Update preferences (push/email/in-app toggles) | ✅ own only |
| `POST` | `/api/v1/users/:id/devices` | Register device token (push) | ✅ own only |
| `DELETE` | `/api/v1/users/:id/devices/:tokenId` | Unregister device token | ✅ own only |
| `GET` | `/api/v1/admin/dlq` | List dead letter queue entries | ✅ admin role |
| `POST` | `/api/v1/admin/dlq/:id/replay` | Replay a DLQ entry | ✅ admin role |
| `GET` | `/health` | Health check | ❌ |
| `GET` | `/metrics` | Prometheus metrics | ❌ (internal) |

---

## Local development (localhost setup)

### Prerequisites

Make sure the following are installed on your machine:

```bash
# Check versions
go version          # 1.22+
docker --version    # 24+
docker-compose --version
migrate --version   # golang-migrate CLI

# Install golang-migrate if missing
brew install golang-migrate   # macOS
# or
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

### Step 1 — Clone and configure

```bash
git clone https://github.com/harmony/notification-svc.git
cd notification-svc

# Copy example env and fill in your keys
cp .env.example .env
```

Open `.env` and fill in the required values:

```env
# ── Database ──────────────────────────────────────────────────────────
DATABASE_URL=postgres://harmony:harmony@localhost:5432/harmony_notification?sslmode=disable

# ── Redis ─────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_GROUP_ID=notification-svc

# ── Push: FCM (get from Firebase Console → Project Settings → Service Accounts)
FCM_PROJECT_ID=your-firebase-project-id
FCM_CREDENTIALS_JSON=/path/to/serviceAccountKey.json
# Or use the JSON content directly:
# FCM_CREDENTIALS_JSON_CONTENT={"type":"service_account",...}

# ── Push: APNs (get from developer.apple.com → Certificates, Identifiers & Profiles)
APNS_KEY_ID=ABCD1234EF
APNS_TEAM_ID=TEAM123456
APNS_KEY_PATH=/path/to/AuthKey_ABCD1234EF.p8
APNS_BUNDLE_ID=app.harmony.ios
APNS_USE_SANDBOX=true   # true for dev, false for prod

# ── Email: SendGrid (get from app.sendgrid.com → Settings → API Keys)
SENDGRID_API_KEY=SG.xxxxxxxxxxxxxxxxxxxx
EMAIL_FROM=noreply@harmony.app
EMAIL_FROM_NAME=Harmony

# ── HTTP ──────────────────────────────────────────────────────────────
HTTP_PORT=8085

# ── gRPC: user-svc (for resolving email addresses)
USER_SERVICE_GRPC=localhost:9083

# ── Retry worker
RETRY_POLL_INTERVAL_MS=10000
RETRY_BATCH_SIZE=50
RETRY_MAX_ATTEMPTS=5

# ── Environment
APP_ENV=development
LOG_LEVEL=debug
```

> **FCM / APNs keys for local testing:** you can skip real push providers by setting `PUSH_PROVIDER=mock` in `.env`, which logs push payloads to stdout instead of calling FCM/APNs.

> **SendGrid for local testing:** set `EMAIL_PROVIDER=mock` to log emails to stdout. Alternatively use [Mailhog](https://github.com/mailhog/MailHog) with a local SMTP adapter.

### Step 2 — Start infrastructure

```bash
# Start PostgreSQL, Kafka, Zookeeper, Redis, and optional Mailhog
docker-compose up -d

# Verify all containers are healthy
docker-compose ps
```

The `docker-compose.yml` starts:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | `5432` | PostgreSQL 15 |
| `redis` | `6379` | Redis 7 |
| `zookeeper` | `2181` | Kafka coordinator |
| `kafka` | `9092` | Apache Kafka |
| `kafka-ui` | `8090` | Kafka UI (browse topics) — optional |
| `mailhog` | `8025` (web) / `1025` (SMTP) | Local email catcher — optional |

### Step 3 — Run database migrations

```bash
migrate \
  -path ./migrations \
  -database "$DATABASE_URL" \
  up

# Verify tables were created
docker exec -it $(docker ps -qf "name=postgres") \
  psql -U harmony -d harmony_notification -c "\dt"
```

Expected output:
```
           List of relations
 Schema |        Name         | Type  |  Owner
--------+---------------------+-------+---------
 public | dead_letter_queue   | table | harmony
 public | delivery_records    | table | harmony
 public | device_tokens       | table | harmony
 public | notifications       | table | harmony
 public | retry_queue         | table | harmony
 public | user_preferences    | table | harmony
```

### Step 4 — Run the service

```bash
go run ./cmd/main.go
```

You should see:

```
2024/01/15 10:00:00 [INFO] notification-svc starting
2024/01/15 10:00:00 [INFO] connected to PostgreSQL
2024/01/15 10:00:00 [INFO] connected to Redis
2024/01/15 10:00:00 [INFO] Kafka consumer started — topics: [identity.user.registered user.friend.request ...]
2024/01/15 10:00:00 [INFO] retry worker started — poll interval: 10s
2024/01/15 10:00:00 [INFO] HTTP server listening on :8085
```

### Step 5 — Verify the service is running

```bash
# Health check
curl http://localhost:8085/health
# → {"status":"ok","kafka":"connected","postgres":"connected","redis":"connected"}

# Metrics (Prometheus format)
curl http://localhost:8085/metrics | grep notification
```

### Step 6 — Smoke test

**Register a device token (simulates a logged-in mobile client):**

```bash
curl -X POST http://localhost:8085/api/v1/users/<userId>/devices \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "token": "fcm-test-token-abc123",
    "platform": "android"
  }'
```

**Publish a test Kafka event (simulates user-svc sending a friend request event):**

```bash
# Using kafka-console-producer from inside the container
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-console-producer \
  --broker-list localhost:9092 \
  --topic user.friend.request << 'EOF'
{"requesterId":"550e8400-e29b-41d4-a716-446655440000","addresseeId":"<userId>","occurredAt":"2024-01-15T10:00:00Z"}
EOF
```

You should see in the service logs:

```
[INFO] consumed event from user.friend.request
[INFO] dispatching notification to userId=<userId> type=friend_request channels=[push in_app]
[INFO] push: sent to 1 android device(s)
[INFO] in-app: published to inapp:<userId>
[INFO] notification saved id=<notifId> status=sent
```

**Fetch notification history:**

```bash
curl http://localhost:8085/api/v1/users/<userId>/notifications \
  -H "Authorization: Bearer <jwt>"
```

**Update preferences (disable email):**

```bash
curl -X PUT http://localhost:8085/api/v1/users/<userId>/preferences \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "push_enabled": true,
    "email_enabled": false,
    "in_app_enabled": true
  }'
```

### Step 7 — View Kafka activity (optional)

Open [http://localhost:8090](http://localhost:8090) in your browser (Kafka UI) to:

- Browse all topics
- View consumer group lag for `notification-svc`
- Manually produce test messages to any topic

### Stopping everything

```bash
# Stop the service (Ctrl+C in terminal running the service)

# Stop and clean up containers
docker-compose down

# Stop and remove volumes (clears all DB data)
docker-compose down -v
```

### Common issues

| Problem | Cause | Fix |
|---|---|---|
| `connection refused :5432` | PostgreSQL not started | `docker-compose up -d postgres` |
| `connection refused :9092` | Kafka not ready yet | Wait 15–20s after `docker-compose up` — Kafka is slow to start |
| `migrate: no migration files found` | Wrong working directory | Run migrate from the repo root |
| `FCM: invalid credential` | Bad service account JSON | Verify `FCM_CREDENTIALS_JSON` path or content |
| Kafka consumer lag > 0 | Service behind on events | Normal on start — check with Kafka UI |
| `dial tcp: redis:6379 — no such host` | Using Docker hostname outside container | Use `localhost:6379` in `.env`, not `redis:6379` |

---

## Testing strategy

```bash
# Unit tests (domain + application — no infrastructure needed)
go test ./domain/... ./application/...

# Integration tests (requires PostgreSQL + Redis + Kafka via Testcontainers)
go test ./infrastructure/... -tags integration

# All tests with coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out

# Run only fast tests (exclude integration)
go test ./... -short
```

| Layer | Type | Dependencies |
|---|---|---|
| `domain/` | Unit | None |
| `application/` | Unit | Mocked ports (`IPushProvider`, `IEmailProvider`, `IPreferenceRepo`, …) |
| `infrastructure/persistence/` | Integration | Real PostgreSQL (Testcontainers) |
| `infrastructure/messaging/` | Integration | Embedded Kafka (Testcontainers) |
| `infrastructure/push/` | Unit | Mock FCM/APNs HTTP server |
| `infrastructure/email/` | Unit | Mock SendGrid HTTP server |

---

## Environment variables

| Variable | Default | Required | Description |
|---|---|---|---|
| `DATABASE_URL` | — | ✅ | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379` | ✅ | Redis connection |
| `KAFKA_BROKERS` | `localhost:9092` | ✅ | Comma-separated Kafka broker addresses |
| `KAFKA_GROUP_ID` | `notification-svc` | — | Kafka consumer group |
| `HTTP_PORT` | `8085` | — | HTTP server port |
| `FCM_PROJECT_ID` | — | Push only | Firebase project ID |
| `FCM_CREDENTIALS_JSON` | — | Push only | Path to service account JSON file |
| `APNS_KEY_ID` | — | iOS push | APNs auth key ID |
| `APNS_TEAM_ID` | — | iOS push | Apple developer team ID |
| `APNS_KEY_PATH` | — | iOS push | Path to `.p8` auth key file |
| `APNS_BUNDLE_ID` | — | iOS push | iOS app bundle ID |
| `APNS_USE_SANDBOX` | `true` | — | `true` = dev APNs, `false` = prod |
| `SENDGRID_API_KEY` | — | Email only | SendGrid API key |
| `EMAIL_FROM` | `noreply@harmony.app` | — | Sender email address |
| `USER_SERVICE_GRPC` | `localhost:9083` | ✅ | user-svc gRPC address (for email resolution) |
| `RETRY_POLL_INTERVAL_MS` | `10000` | — | Retry worker poll interval (ms) |
| `RETRY_BATCH_SIZE` | `50` | — | Max retries per poll cycle |
| `RETRY_MAX_ATTEMPTS` | `5` | — | Attempts before DLQ |
| `PUSH_PROVIDER` | `fcm` | — | `fcm` / `mock` (mock logs to stdout) |
| `EMAIL_PROVIDER` | `sendgrid` | — | `sendgrid` / `mock` |
| `LOG_LEVEL` | `info` | — | `debug` / `info` / `warn` / `error` |
| `APP_ENV` | `development` | — | `development` / `staging` / `production` |

---

## Related services

| Service | Relationship |
|---|---|
| `auth-svc` | Publishes `identity.user.registered` and `identity.user.loggedin` |
| `user-svc` | Publishes `user.friend.request` and `user.friend.accepted`; called via gRPC to resolve email address |
| `community-svc` | Publishes `community.server.created`, `community.member.joined`, `community.role.assigned` |
| `ws-gateway` | Subscribes to Redis `inapp:{userId}` pub/sub channel and fans out to open WebSocket connections |
| `api-gateway` | Routes `GET /notifications` and `PUT /preferences` to this service |

---

*Part of the Harmony platform monorepo — see the root [README](../../README.md) for the full architecture overview.*
# 事件系统使用指南（简化版）

## 概述

`pkg/event` 提供了类似 ThinkPHP 的简洁事件 API，支持：

- **同步事件**：本地监听器，立即执行
- **异步事件**：通过 Kafka 消息队列，跨服务通信
- **高可用**：重试、降级、超时保护

## 快速开始

### 1. 初始化事件系统

```go
import "github.com/EladiaShipmenyrt89/think-event"

// 创建Kafka事件总线
eventBus, _ := event.NewKafkaEventBus(
    []string{"localhost:9092"},
    "user-events",
    "users-service",
    logger,
)

// 初始化全局事件门面
event.Init(eventBus)

// 启动事件总线（启动消费者）
event.Start(context.Background())
```

### 2. 注册事件监听器

```go
// 方式1：注册本地监听器（同步执行）
event.Listen("user.created", func(ctx context.Context, evt event.Event) error {
    userEvent := evt.Payload().(*UserCreatedEvent)
    fmt.Printf("用户创建: %s\n", userEvent.UserID)
    return nil
})

// 方式2：注册异步监听器（通过消息队列）
event.ListenAsync("user.created", func(ctx context.Context, evt event.Event) error {
    // 这个监听器会在其他服务中执行
    userEvent := evt.Payload().(*UserCreatedEvent)
    return sendWelcomeEmail(userEvent.Email)
})

// 方式3：批量注册
event.ListenEvents(map[string]event.EventHandler{
    "user.created": func(ctx context.Context, evt event.Event) error {
        // 处理用户创建事件
        return nil
    },
    "user.updated": func(ctx context.Context, evt event.Event) error {
        // 处理用户更新事件
        return nil
    },
})
```

### 3. 触发事件

```go
// 方式1：检查是否有监听器再触发（类似ThinkPHP）
if event.HasListener("userlog_ip_add") {
    event.Trigger(ctx, "userlog_ip_add", &UserLogIPAddEvent{
        UserID:   user.ID,
        Username: user.Username,
        ClientIP: clientIP,
    })
}

// 方式2：直接触发（推荐）
event.Trigger(ctx, "user.created", &UserCreatedEvent{
    UserID:    user.ID,
    Username:  user.Username,
    TenantID:  tenantID,
    EventTime: time.Now(),
})

// 方式3：异步触发（只发布到消息队列）
event.TriggerAsync(ctx, "user.created", &UserCreatedEvent{...})
```

## 完整示例（类似 ThinkPHP）

```go
package main

import (
    "context"
    "time"

   "github.com/EladiaShipmenyrt89/think-event"
    "go.uber.org/zap"
)

// 定义事件
type UserLogIPAddEvent struct {
    UserID   string    `json:"user_id"`
    Username string    `json:"username"`
    ClientIP string    `json:"client_ip"`
    TenantID string    `json:"tenant_id"`
    EventTime time.Time `json:"timestamp"`
}

func (e *UserLogIPAddEvent) Type() string {
    return "userlog_ip_add"
}

func (e *UserLogIPAddEvent) Payload() interface{} {
    return e
}

func (e *UserLogIPAddEvent) GetTenantID() string {
    return e.TenantID
}

func (e *UserLogIPAddEvent) Timestamp() time.Time {
    return e.EventTime
}

func main() {
    // 1. 初始化事件系统
    eventBus, _ := event.NewKafkaEventBus(
        []string{"localhost:9092"},
        "user-events",
        "users-service",
        zap.NewNop(),
    )
    event.Init(eventBus)
    event.Start(context.Background())

    // 2. 注册事件监听器
    event.Listen("userlog_ip_add", func(ctx context.Context, evt event.Event) error {
        payload := evt.Payload().(*UserLogIPAddEvent)
        fmt.Printf("记录IP: user=%s, ip=%s\n", payload.Username, payload.ClientIP)
        return nil
    })

    // 3. 触发事件（类似ThinkPHP的写法）
    ctx := context.Background()
    user := &User{ID: "123", Username: "testuser"}
    clientIP := "192.168.1.100"

    // 检查是否有监听器再触发
    if event.HasListener("userlog_ip_add") {
        event.Trigger(ctx, "userlog_ip_add", &UserLogIPAddEvent{
            UserID:   user.ID,
            Username: user.Username,
            ClientIP: clientIP,
            TenantID: "tenant-123",
            EventTime: time.Now(),
        })
    }
}
```

## 事件别名（类似 ThinkPHP 的 bind）

```go
// 绑定事件别名，便于调用
event.Bind(map[string]string{
    "user_create": "user.created",
    "user_update": "user.updated",
    "ip_add":      "userlog_ip_add",
})

// 使用别名触发事件
event.Trigger(ctx, "ip_add", &UserLogIPAddEvent{...})
```

## 事件订阅者（批量注册）

```go
// 定义事件订阅者
type UserEventSubscriber struct{}

func (s *UserEventSubscriber) Subscribe() map[string]event.EventHandler {
    return map[string]event.EventHandler{
        "user.created": func(ctx context.Context, evt event.Event) error {
            // 处理用户创建
            return nil
        },
        "user.updated": func(ctx context.Context, evt event.Event) error {
            // 处理用户更新
            return nil
        },
    }
}

// 注册订阅者
event.Subscribe(&UserEventSubscriber{})
```

## 高可用模式

```go
import "github.com/EladiaShipmenyrt89/think-event"

// 创建高可用事件门面
haFacade := event.NewHighAvailabilityEventFacade(
    eventBus,
    &event.HighAvailabilityConfig{
        MaxRetries:      5,              // 最大重试5次
        RetryInterval:  time.Second,    // 重试间隔1秒
        RetryBackoff:   true,            // 使用指数退避
        FallbackEnabled: true,           // 启用降级
        FallbackHandler: func(ctx context.Context, evt event.Event) error {
            // 降级处理：记录到本地日志或数据库
            return logToLocal(evt)
        },
        Timeout: 5 * time.Second,        // 超时5秒
    },
)

// 使用高可用触发
haFacade.TriggerWithHA(ctx, "user.created", &UserCreatedEvent{...})
```

## API 对比（ThinkPHP vs 本系统）

| ThinkPHP                            | 本系统                               | 说明             |
| ----------------------------------- | ------------------------------------ | ---------------- |
| `Event::hasListener('event')`       | `event.HasListener("event")`         | 检查是否有监听器 |
| `Event::trigger('event', $data)`    | `event.Trigger(ctx, "event", evt)`   | 触发事件         |
| `Event::listen('event', $handler)`  | `event.Listen("event", handler)`     | 注册监听器       |
| `Event::bind(['alias' => 'event'])` | `event.Bind(map[string]string{...})` | 绑定别名         |
| `Event::subscribe($subscriber)`     | `event.Subscribe(subscriber)`        | 注册订阅者       |

## 优势

1. **简洁 API**：类似 ThinkPHP，易于使用
2. **高可用**：重试、降级、超时保护
3. **灵活**：支持同步和异步事件
4. **跨服务**：通过 Kafka 支持跨服务通信
5. **向后兼容**：可以与现有 Hook 系统集成

## 与 Hook 系统集成

```go
// 在Repository Hook中使用事件系统
type UserHook struct{}

func (h *UserHook) AfterCreateAsync(ctx context.Context, user *User) error {
    // 使用事件系统触发事件
    if event.HasListener("user.created") {
        return event.TriggerAsync(ctx, "user.created", &UserCreatedEvent{
            UserID:    user.ID,
            Username:  user.Username,
            TenantID:  user.TenantID,
            EventTime: time.Now(),
        })
    }
    return nil
}
```

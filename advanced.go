package event

import (
	"context"
	"time"
)

// AdvancedEventBus 高级事件总线接口（扩展EventBus）
// 支持延迟消息、事务事件、持久化日志等高级特性
type AdvancedEventBus interface {
	EventBus // 继承基础EventBus接口

	// PublishDelayed 发布延迟消息
	// delay: 延迟时间
	// 示例: bus.PublishDelayed(ctx, event, 5*time.Minute)
	PublishDelayed(ctx context.Context, event Event, delay time.Duration) error

	// PublishTransactional 发布事务事件（需要事务支持）
	// 事务提交后才发布事件，事务回滚则取消发布
	// 示例: bus.PublishTransactional(ctx, tx, event)
	PublishTransactional(ctx context.Context, tx interface{}, event Event) error

	// PublishWithOptions 发布事件（带选项）
	// 示例: bus.PublishWithOptions(ctx, event, &PublishOptions{...})
	PublishWithOptions(ctx context.Context, event Event, options *PublishOptions) error

	// SubscribeWithOptions 订阅事件（带选项）
	// 示例: bus.SubscribeWithOptions("event", handler, &SubscribeOptions{...})
	SubscribeWithOptions(eventType string, handler EventHandler, options *SubscribeOptions) error
}

// PublishOptions 发布选项
type PublishOptions struct {
	// 延迟时间（如果设置了，等同于PublishDelayed）
	Delay time.Duration

	// 消息键（用于分区路由）
	Key string

	// 消息头
	Headers map[string]string

	// 优先级（某些消息代理支持）
	Priority int

	// 过期时间
	TTL time.Duration

	// 是否持久化
	Persistent bool

	// 事务（如果设置了，等同于PublishTransactional）
	Transaction interface{}
}

// SubscribeOptions 订阅选项
type SubscribeOptions struct {
	// 消费者组ID
	GroupID string

	// 是否持久化订阅
	Persistent bool

	// 重试配置
	MaxRetries    int
	RetryInterval time.Duration

	// 并发数
	Concurrency int

	// 批量大小（批量消费）
	BatchSize int

	// 过滤器（只处理符合条件的事件）
	Filter func(Event) bool
}

// EventLog 事件日志接口（持久化事件日志）
type EventLog interface {
	// Append 追加事件到日志
	Append(ctx context.Context, event Event) error

	// Read 读取事件日志（支持分页）
	Read(ctx context.Context, eventType string, offset, limit int64) ([]Event, error)

	// ReadSince 读取指定时间之后的事件
	ReadSince(ctx context.Context, eventType string, since time.Time) ([]Event, error)

	// GetOffset 获取当前偏移量
	GetOffset(ctx context.Context, eventType string) (int64, error)
}

// TransactionalEventBus 事务事件总线接口
type TransactionalEventBus interface {
	AdvancedEventBus

	// BeginTransaction 开始事务
	BeginTransaction(ctx context.Context) (interface{}, error)

	// CommitTransaction 提交事务（提交后事件才会发布）
	CommitTransaction(ctx context.Context, tx interface{}) error

	// RollbackTransaction 回滚事务（取消事件发布）
	RollbackTransaction(ctx context.Context, tx interface{}) error
}

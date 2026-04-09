package event

import (
	"context"
	"time"
)

// EventBus 事件总线接口
type EventBus interface {
	// Publish 发布事件
	Publish(ctx context.Context, event Event) error
	// Subscribe 订阅事件
	Subscribe(eventType string, handler EventHandler) error
	// Start 启动事件总线（启动消费者）
	Start(ctx context.Context) error
	// Stop 停止事件总线
	Stop() error
}

// Event 事件接口
type Event interface {
	// Type 返回事件类型
	Type() string
	// Payload 返回事件负载
	Payload() interface{}
	// Timestamp 返回事件时间戳
	Timestamp() time.Time
}

// EventHandler 事件处理器
type EventHandler func(ctx context.Context, event Event) error

package event

import (
	"context"
	"fmt"
	"sync"
)

// EventFacade 事件门面（简化API）
// 提供类似ThinkPHP的简洁API
type EventFacade struct {
	bus EventBus
	mu  sync.RWMutex
	// 本地事件监听器（同步执行，不经过消息队列）
	localListeners map[string][]EventHandler
	// 事件别名映射
	aliases map[string]string
}

// NewEventFacade 创建事件门面
func NewEventFacade(bus EventBus) *EventFacade {
	return &EventFacade{
		bus:            bus,
		localListeners: make(map[string][]EventHandler),
		aliases:        make(map[string]string),
	}
}

// Listen 注册事件监听（本地监听器，同步执行）
// 示例: Event.Listen("user.created", func(ctx context.Context, evt Event) error { ... })
func (f *EventFacade) Listen(eventType string, handler EventHandler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localListeners[eventType] = append(f.localListeners[eventType], handler)
}

// ListenAsync 注册异步事件监听（通过消息队列）
// 示例: Event.ListenAsync("user.created", func(ctx context.Context, evt Event) error { ... })
func (f *EventFacade) ListenAsync(eventType string, handler EventHandler) error {
	if f.bus == nil {
		return fmt.Errorf("event bus not configured")
	}
	return f.bus.Subscribe(eventType, handler)
}

// HasListener 检查是否存在事件监听器
// 示例: if Event.HasListener("user.created") { ... }
func (f *EventFacade) HasListener(eventType string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// 检查别名
	if alias, ok := f.aliases[eventType]; ok {
		eventType = alias
	}

	// 检查本地监听器
	if len(f.localListeners[eventType]) > 0 {
		return true
	}

	// 如果有事件总线，认为可能有远程监听器
	return f.bus != nil
}

// Trigger 触发事件（同步执行本地监听器，异步发布到消息队列）
// 示例: Event.Trigger("user.created", &UserCreatedEvent{UserID: "123"})
func (f *EventFacade) Trigger(ctx context.Context, eventType string, event Event) error {
	resolvedType, err := f.resolveEventType(eventType, event)
	if err != nil {
		return err
	}

	// 1. 同步执行本地监听器
	f.mu.RLock()
	localHandlers := f.localListeners[resolvedType]
	f.mu.RUnlock()

	var firstErr error
	for _, handler := range localHandlers {
		if err := handler(ctx, event); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// 继续执行其他监听器
		}
	}

	// 2. 异步发布到消息队列（如果有事件总线）
	if f.bus != nil {
		// 在goroutine中发布，不阻塞主流程
		go func() {
			if err := f.bus.Publish(ctx, event); err != nil {
				// 事件发布失败不影响主流程，可以记录日志
				_ = err
			}
		}()
	}

	return firstErr
}

// TriggerEvent 根据事件对象自身的 Type 触发事件，避免重复传 eventType。
func (f *EventFacade) TriggerEvent(ctx context.Context, event Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	return f.Trigger(ctx, event.Type(), event)
}

// TriggerAsync 异步触发事件（只发布到消息队列，不执行本地监听器）
// 示例: Event.TriggerAsync("user.created", &UserCreatedEvent{UserID: "123"})
func (f *EventFacade) TriggerAsync(ctx context.Context, eventType string, event Event) error {
	if f.bus == nil {
		return fmt.Errorf("event bus not configured")
	}

	if _, err := f.resolveEventType(eventType, event); err != nil {
		return err
	}

	return f.bus.Publish(ctx, event)
}

// TriggerAsyncEvent 根据事件对象自身的 Type 异步触发事件。
func (f *EventFacade) TriggerAsyncEvent(ctx context.Context, event Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	return f.TriggerAsync(ctx, event.Type(), event)
}

// Remove 移除事件监听器
func (f *EventFacade) Remove(eventType string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.localListeners, eventType)
}

// Bind 绑定事件别名
// 示例: Event.Bind(map[string]string{"user_create": "user.created"})
func (f *EventFacade) Bind(aliases map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for alias, eventType := range aliases {
		f.aliases[alias] = eventType
	}
}

// Subscribe 注册事件订阅者（批量注册）
// 示例: Event.Subscribe(&UserEventSubscriber{})
type EventSubscriber interface {
	// Subscribe 返回事件类型到处理器的映射
	Subscribe() map[string]EventHandler
}

func (f *EventFacade) Subscribe(subscriber EventSubscriber) error {
	handlers := subscriber.Subscribe()
	for eventType, handler := range handlers {
		if err := f.ListenAsync(eventType, handler); err != nil {
			return fmt.Errorf("failed to subscribe %s: %w", eventType, err)
		}
	}
	return nil
}

// ListenEvents 批量注册事件监听
func (f *EventFacade) ListenEvents(events map[string]EventHandler) {
	for eventType, handler := range events {
		f.Listen(eventType, handler)
	}
}

// Until 触发事件并获取第一个有效返回值（类似ThinkPHP的until方法）
// 返回第一个非nil的返回值
func (f *EventFacade) Until(ctx context.Context, eventType string, event Event) (interface{}, error) {
	// 检查别名
	if alias, ok := f.aliases[eventType]; ok {
		eventType = alias
	}

	f.mu.RLock()
	localHandlers := f.localListeners[eventType]
	f.mu.RUnlock()

	// 执行本地监听器，返回第一个非nil结果
	for _, handler := range localHandlers {
		// 注意：这里需要修改EventHandler接口才能返回值
		// 暂时只执行，不返回值
		if err := handler(ctx, event); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// Start 启动事件总线（启动消费者）
func (f *EventFacade) Start(ctx context.Context) error {
	if f.bus == nil {
		return nil
	}
	return f.bus.Start(ctx)
}

// Stop 停止事件总线
func (f *EventFacade) Stop() error {
	if f.bus == nil {
		return nil
	}
	return f.bus.Stop()
}

func (f *EventFacade) resolveEventType(eventType string, event Event) (string, error) {
	if event == nil {
		return "", fmt.Errorf("event is nil")
	}

	resolvedType := eventType

	f.mu.RLock()
	if alias, ok := f.aliases[eventType]; ok {
		resolvedType = alias
	}
	f.mu.RUnlock()

	if resolvedType == "" {
		resolvedType = event.Type()
	}

	if resolvedType != event.Type() {
		return "", fmt.Errorf("event type mismatch: arg=%s event=%s", resolvedType, event.Type())
	}

	return resolvedType, nil
}

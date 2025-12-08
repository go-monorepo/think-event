package event

import (
	"context"
	"sync"
)

// Event 全局事件门面实例（单例模式）
var (
	globalFacade *EventFacade
	facadeOnce   sync.Once
	facadeMu     sync.RWMutex
)

// Init 初始化全局事件门面
func Init(bus EventBus) {
	facadeOnce.Do(func() {
		globalFacade = NewEventFacade(bus)
	})
}

// GetFacade 获取全局事件门面
func GetFacade() *EventFacade {
	facadeMu.RLock()
	defer facadeMu.RUnlock()
	if globalFacade == nil {
		// 如果没有初始化，创建一个不带事件总线的门面（仅本地监听器）
		globalFacade = NewEventFacade(nil)
	}
	return globalFacade
}

// SetFacade 设置全局事件门面（用于测试）
func SetFacade(facade *EventFacade) {
	facadeMu.Lock()
	defer facadeMu.Unlock()
	globalFacade = facade
}

// 全局便捷函数（类似ThinkPHP的Event）

// Listen 注册事件监听
func Listen(eventType string, handler EventHandler) {
	GetFacade().Listen(eventType, handler)
}

// ListenAsync 注册异步事件监听
func ListenAsync(eventType string, handler EventHandler) error {
	return GetFacade().ListenAsync(eventType, handler)
}

// HasListener 检查是否存在事件监听器
func HasListener(eventType string) bool {
	return GetFacade().HasListener(eventType)
}

// Trigger 触发事件
func Trigger(ctx context.Context, eventType string, event Event) error {
	return GetFacade().Trigger(ctx, eventType, event)
}

// TriggerAsync 异步触发事件
func TriggerAsync(ctx context.Context, eventType string, event Event) error {
	return GetFacade().TriggerAsync(ctx, eventType, event)
}

// Remove 移除事件监听器
func Remove(eventType string) {
	GetFacade().Remove(eventType)
}

// Bind 绑定事件别名
func Bind(aliases map[string]string) {
	GetFacade().Bind(aliases)
}

// Subscribe 注册事件订阅者
func Subscribe(subscriber EventSubscriber) error {
	return GetFacade().Subscribe(subscriber)
}

// ListenEvents 批量注册事件监听
func ListenEvents(events map[string]EventHandler) {
	GetFacade().ListenEvents(events)
}

// Until 触发事件并获取第一个有效返回值
func Until(ctx context.Context, eventType string, event Event) (interface{}, error) {
	return GetFacade().Until(ctx, eventType, event)
}

// Start 启动事件总线
func Start(ctx context.Context) error {
	return GetFacade().Start(ctx)
}

// Stop 停止事件总线
func Stop() error {
	return GetFacade().Stop()
}

package event

import (
	"context"
	"testing"
	"time"
)

type facadeTestEvent struct {
	eventType string
	payload   interface{}
	timestamp time.Time
}

func (e *facadeTestEvent) Type() string {
	return e.eventType
}

func (e *facadeTestEvent) Payload() interface{} {
	return e.payload
}

func (e *facadeTestEvent) Timestamp() time.Time {
	return e.timestamp
}

type facadeTestBus struct {
	published []Event
}

func (b *facadeTestBus) Publish(_ context.Context, event Event) error {
	b.published = append(b.published, event)
	return nil
}

func (b *facadeTestBus) Subscribe(string, EventHandler) error {
	return nil
}

func (b *facadeTestBus) Start(context.Context) error {
	return nil
}

func (b *facadeTestBus) Stop() error {
	return nil
}

func TestTriggerEventUsesEventType(t *testing.T) {
	t.Parallel()

	facade := NewEventFacade(nil)
	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	called := false
	facade.Listen("user.created", func(ctx context.Context, evt Event) error {
		called = true
		if evt != event {
			t.Fatalf("unexpected event instance: %p", evt)
		}
		return nil
	})

	if err := facade.TriggerEvent(context.Background(), event); err != nil {
		t.Fatalf("TriggerEvent returned error: %v", err)
	}
	if !called {
		t.Fatal("local handler was not called")
	}
}

func TestTriggerAsyncEventPublishesEvent(t *testing.T) {
	t.Parallel()

	bus := &facadeTestBus{}
	facade := NewEventFacade(bus)
	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	if err := facade.TriggerAsyncEvent(context.Background(), event); err != nil {
		t.Fatalf("TriggerAsyncEvent returned error: %v", err)
	}
	if len(bus.published) != 1 {
		t.Fatalf("unexpected publish count: %d", len(bus.published))
	}
	if bus.published[0] != event {
		t.Fatalf("unexpected published event instance: %p", bus.published[0])
	}
}

func TestTriggerAsyncRejectsMismatchedEventType(t *testing.T) {
	t.Parallel()

	bus := &facadeTestBus{}
	facade := NewEventFacade(bus)
	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	err := facade.TriggerAsync(context.Background(), "user.updated", event)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if len(bus.published) != 0 {
		t.Fatalf("event should not be published on mismatch: %d", len(bus.published))
	}
}

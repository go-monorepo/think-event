package event

import (
	"context"
	"testing"
	"time"
)

type testPayload struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type stubBus struct {
	subscribe func(eventType string, handler EventHandler) error
}

type localEvent struct {
	eventType string
	payload   interface{}
	timestamp time.Time
}

func (b *stubBus) Publish(context.Context, Event) error {
	return nil
}

func (b *stubBus) Subscribe(eventType string, handler EventHandler) error {
	if b.subscribe == nil {
		return nil
	}
	return b.subscribe(eventType, handler)
}

func (b *stubBus) Start(context.Context) error {
	return nil
}

func (b *stubBus) Stop() error {
	return nil
}

func (e *localEvent) Type() string {
	return e.eventType
}

func (e *localEvent) Payload() interface{} {
	return e.payload
}

func (e *localEvent) Timestamp() time.Time {
	return e.timestamp
}

func TestPayloadAsDecodesRawPayload(t *testing.T) {
	t.Parallel()

	evt := &genericEvent{
		eventType: "user.created",
		payload: map[string]interface{}{
			"user_id": "u-1",
		},
		rawPayload: []byte(`{"user_id":"u-1","email":"test@example.com"}`),
		timestamp:  time.Now(),
	}

	payload, err := PayloadAs[testPayload](evt)
	if err != nil {
		t.Fatalf("PayloadAs returned error: %v", err)
	}

	if payload.UserID != "u-1" {
		t.Fatalf("unexpected user id: %s", payload.UserID)
	}
	if payload.Email != "test@example.com" {
		t.Fatalf("unexpected email: %s", payload.Email)
	}
}

func TestPayloadAsFallsBackToPayloadValue(t *testing.T) {
	t.Parallel()

	evt := &localEvent{
		eventType: "lottery.settle-status.trigger",
		payload: map[string]interface{}{
			"user_id": "u-2",
			"email":   "fallback@example.com",
		},
		timestamp: time.Now(),
	}

	payload, err := PayloadAs[testPayload](evt)
	if err != nil {
		t.Fatalf("PayloadAs returned error: %v", err)
	}

	if payload.UserID != "u-2" {
		t.Fatalf("unexpected user id: %s", payload.UserID)
	}
	if payload.Email != "fallback@example.com" {
		t.Fatalf("unexpected email: %s", payload.Email)
	}
}

func TestSubscribeJSONWrapsTypedPayload(t *testing.T) {
	t.Parallel()

	bus := &stubBus{
		subscribe: func(eventType string, handler EventHandler) error {
			if eventType != "user.created" {
				t.Fatalf("unexpected event type: %s", eventType)
			}

			return handler(context.Background(), &genericEvent{
				eventType: "user.created",
				payload: map[string]interface{}{
					"user_id": "u-1",
				},
				rawPayload: []byte(`{"user_id":"u-1","email":"test@example.com"}`),
				timestamp:  time.Now(),
			})
		},
	}

	called := false
	err := SubscribeJSON[testPayload](bus, "user.created", func(ctx context.Context, payload *testPayload, evt Event) error {
		called = true

		if payload.Email != "test@example.com" {
			t.Fatalf("unexpected payload email: %s", payload.Email)
		}

		typedPayload, ok := evt.Payload().(*testPayload)
		if !ok {
			t.Fatalf("unexpected payload type: %T", evt.Payload())
		}
		if typedPayload.UserID != "u-1" {
			t.Fatalf("unexpected user id: %s", typedPayload.UserID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("SubscribeJSON returned error: %v", err)
	}
	if !called {
		t.Fatal("typed handler was not called")
	}
}

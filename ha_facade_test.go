package event

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type haTestBus struct {
	mu          sync.Mutex
	publishErrs []error
	publishCnt  int
	published   []Event
	publishCh   chan struct{}
}

func newHATestBus(publishErrs ...error) *haTestBus {
	return &haTestBus{
		publishErrs: publishErrs,
		publishCh:   make(chan struct{}, 16),
	}
}

func (b *haTestBus) Publish(_ context.Context, event Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx := b.publishCnt
	b.publishCnt++
	b.published = append(b.published, event)

	var err error
	if idx < len(b.publishErrs) {
		err = b.publishErrs[idx]
	}

	b.publishCh <- struct{}{}
	return err
}

func (b *haTestBus) Subscribe(string, EventHandler) error {
	return nil
}

func (b *haTestBus) Start(context.Context) error {
	return nil
}

func (b *haTestBus) Stop() error {
	return nil
}

func (b *haTestBus) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.publishCnt
}

func (b *haTestBus) waitForCalls(t *testing.T, want int) {
	t.Helper()

	timeout := time.After(500 * time.Millisecond)
	for {
		if got := b.calls(); got >= want {
			return
		}

		select {
		case <-b.publishCh:
		case <-timeout:
			t.Fatalf("timed out waiting for %d publish calls, got %d", want, b.calls())
		}
	}
}

func TestTriggerWithHAPublishesToBusOnce(t *testing.T) {
	t.Parallel()

	bus := newHATestBus(nil)
	facade := NewHighAvailabilityEventFacade(bus, &HighAvailabilityConfig{
		MaxRetries:      0,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	})

	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	localCalls := 0
	facade.Listen("user.created", func(context.Context, Event) error {
		localCalls++
		return nil
	})

	if err := facade.TriggerWithHA(context.Background(), "user.created", event); err != nil {
		t.Fatalf("TriggerWithHA returned error: %v", err)
	}

	bus.waitForCalls(t, 1)
	time.Sleep(50 * time.Millisecond)

	if got := bus.calls(); got != 1 {
		t.Fatalf("unexpected publish count: %d", got)
	}
	if localCalls != 1 {
		t.Fatalf("unexpected local handler count: %d", localCalls)
	}
}

func TestTriggerAsyncWithHARetriesMaxRetriesPlusInitialAttempt(t *testing.T) {
	t.Parallel()

	bus := newHATestBus(
		errors.New("first publish failed"),
		errors.New("second publish failed"),
		nil,
	)
	facade := NewHighAvailabilityEventFacade(bus, &HighAvailabilityConfig{
		MaxRetries:      2,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	})

	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	if err := facade.TriggerAsyncWithHA(context.Background(), "user.created", event); err != nil {
		t.Fatalf("TriggerAsyncWithHA returned error: %v", err)
	}

	bus.waitForCalls(t, 3)
	time.Sleep(20 * time.Millisecond)

	if got := bus.calls(); got != 3 {
		t.Fatalf("unexpected publish count: %d", got)
	}
}

func TestTriggerAsyncWithHARejectsMismatchedEventType(t *testing.T) {
	t.Parallel()

	bus := newHATestBus(nil)
	facade := NewHighAvailabilityEventFacade(bus, &HighAvailabilityConfig{
		MaxRetries:      1,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	})

	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	err := facade.TriggerAsyncWithHA(context.Background(), "user.updated", event)
	if err == nil {
		t.Fatal("expected mismatch error")
	}

	time.Sleep(20 * time.Millisecond)
	if got := bus.calls(); got != 0 {
		t.Fatalf("event should not be published on mismatch: %d", got)
	}
}

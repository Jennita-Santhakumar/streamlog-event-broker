package handlers

import (
	"log"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// tailBroadcaster fans out every published message to any WebSocket clients
// currently tailing that topic. The PRD lists "real-time processing with
// WebSocket subscriptions" as a feature but its own router sketch never
// wires an endpoint for it — this is that missing piece, implemented on top
// of the same Publish path the REST producer API uses (not a separate
// queue), so a tailing client sees exactly what got durably written.
type tailBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan tailEvent]struct{} // topic -> set of subscriber channels
}

type tailEvent struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     interface{}       `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp int64             `json:"timestamp"`
}

func newTailBroadcaster() *tailBroadcaster {
	return &tailBroadcaster{subscribers: make(map[string]map[chan tailEvent]struct{})}
}

func (b *tailBroadcaster) subscribe(topic string) chan tailEvent {
	ch := make(chan tailEvent, 32)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscribers[topic] == nil {
		b.subscribers[topic] = make(map[chan tailEvent]struct{})
	}
	b.subscribers[topic][ch] = struct{}{}
	return ch
}

func (b *tailBroadcaster) unsubscribe(topic string, ch chan tailEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if set, ok := b.subscribers[topic]; ok {
		delete(set, ch)
	}
	close(ch)
}

func (b *tailBroadcaster) publish(topic string, partition int, offset int64, key string, value interface{}, headers map[string]string, timestamp int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	evt := tailEvent{Topic: topic, Partition: partition, Offset: offset, Key: key, Value: value, Headers: headers, Timestamp: timestamp}
	for ch := range b.subscribers[topic] {
		select {
		case ch <- evt:
		default: // slow subscriber: drop rather than block publishers
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// TailEvents upgrades to a WebSocket and streams every message published to
// the topic from this point forward.
func (h *Handler) TailEvents(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")
	if _, err := h.Broker.PartitionCount(topic); err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	ch := h.tailBroadcaster.subscribe(topic)
	defer h.tailBroadcaster.unsubscribe(topic, ch)

	for evt := range ch {
		if err := conn.WriteJSON(evt); err != nil {
			return
		}
	}
}

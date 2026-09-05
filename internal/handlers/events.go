package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/metrics"
	"github.com/go-chi/chi/v5"
)

type publishRequest struct {
	Key     string            `json:"key"`
	Value   json.RawMessage   `json:"value"`
	Headers map[string]string `json:"headers"`
}

func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.Value) == 0 {
		writeError(w, http.StatusBadRequest, "value is required")
		return
	}

	start := time.Now()
	partition, offset, err := h.Broker.Publish(r.Context(), topic, req.Key, req.Value, req.Headers)
	if err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	metrics.PublishLatency.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	metrics.MessagesPublished.WithLabelValues(topic, itoa(partition)).Inc()

	h.tailBroadcaster.publish(topic, partition, offset, req.Key, req.Value, req.Headers, time.Now().UnixMilli())

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
		"timestamp": time.Now().UnixMilli(),
	})
}

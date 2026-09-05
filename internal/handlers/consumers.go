package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/broker"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/metrics"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
	"github.com/go-chi/chi/v5"
)

type consumedMessageDTO struct {
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     json.RawMessage   `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp int64             `json:"timestamp"`
}

// PullEvents implements the pull-based consumer API: GET
// /api/v1/consumers/{group}/events/{topic}?partition=N&offset=(N|earliest|latest)&max_bytes=N
//
// If offset is omitted, it resumes from the group's last committed offset
// for that partition (defaulting to 0 if the group has never committed).
func (h *Handler) PullEvents(w http.ResponseWriter, r *http.Request) {
	group := chi.URLParam(r, "group")
	topic := chi.URLParam(r, "topic")

	partition, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "partition query param is required")
		return
	}

	maxBytes := 1 << 20 // 1MB default fetch size
	if v := r.URL.Query().Get("max_bytes"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxBytes = n
		}
	}

	fromOffset, err := h.resolveOffset(r, group, topic, partition)
	if err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}

	start := time.Now()
	messages, err := h.Broker.Consume(r.Context(), topic, partition, fromOffset, maxBytes)
	if err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	metrics.ConsumeLatency.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	metrics.MessagesConsumed.WithLabelValues(topic, itoa(partition)).Add(float64(len(messages)))

	dtos := make([]consumedMessageDTO, 0, len(messages))
	for _, m := range messages {
		dtos = append(dtos, consumedMessageDTO{
			Offset: m.Offset, Key: m.Key, Value: m.Value, Headers: m.Headers, Timestamp: m.Timestamp,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"messages":  dtos,
	})
}

func (h *Handler) resolveOffset(r *http.Request, group, topic string, partition int) (int64, error) {
	raw := r.URL.Query().Get("offset")
	switch raw {
	case "earliest":
		return 0, nil
	case "latest":
		return h.Broker.LatestOffset(topic, partition)
	case "":
		offset, found, err := h.Broker.CommittedOffset(r.Context(), group, topic, partition)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, nil
		}
		return offset, nil
	default:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, broker.ErrPartitionOutOfRange
		}
		return n, nil
	}
}

type commitRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// CommitOffset implements POST /api/v1/consumers/{group}/commit — a consumer
// explicitly acknowledges it has processed up to and including `offset`, so
// the next PullEvents call without an explicit offset resumes after it.
func (h *Handler) CommitOffset(w http.ResponseWriter, r *http.Request) {
	group := chi.URLParam(r, "group")

	var req commitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.Broker.CommitOffset(r.Context(), group, req.Topic, req.Partition, req.Offset); err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "committed"})
}

type dlqRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Reason    string `json:"reason"`
}

// ReportDLQ implements POST /api/v1/dlq — the PRD's schema includes a dlq
// table but never defines how a message lands in it. A consumer that gives
// up retrying a message reports it here explicitly.
func (h *Handler) ReportDLQ(w http.ResponseWriter, r *http.Request) {
	var req dlqRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Reason == "" {
		req.Reason = "unspecified"
	}

	if err := h.Broker.ReportDLQ(r.Context(), req.Topic, req.Partition, req.Offset, req.Reason); err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	metrics.DLQMessages.WithLabelValues(req.Topic).Inc()
	writeJSON(w, http.StatusCreated, map[string]string{"status": "recorded"})
}

func (h *Handler) ListDLQ(w http.ResponseWriter, r *http.Request) {
	rows, err := h.Broker.ListDLQ(r.Context(), 200)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rows == nil {
		rows = []storage.DLQRow{}
	}
	writeJSON(w, http.StatusOK, rows)
}

func (h *Handler) ConsumerLag(w http.ResponseWriter, r *http.Request) {
	rows, err := h.Broker.ConsumerLag(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rows == nil {
		rows = []storage.ConsumerLagRow{}
	}
	writeJSON(w, http.StatusOK, rows)
}

package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
	"github.com/go-chi/chi/v5"
)

type createTopicRequest struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	RetentionMs       int64  `json:"retention_ms"`
}

func (h *Handler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Partitions <= 0 {
		req.Partitions = 3
	}
	if req.ReplicationFactor <= 0 {
		req.ReplicationFactor = 3
	}
	if req.RetentionMs <= 0 {
		req.RetentionMs = 86400000
	}

	t, err := h.Broker.CreateTopic(r.Context(), req.Name, req.Partitions, req.ReplicationFactor, req.RetentionMs)
	if err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := h.Broker.ListTopics(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if topics == nil {
		topics = []storage.Topic{}
	}
	writeJSON(w, http.StatusOK, topics)
}

func (h *Handler) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")
	if err := h.Broker.DeleteTopic(r.Context(), topic); err != nil {
		writeError(w, statusForBrokerErr(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

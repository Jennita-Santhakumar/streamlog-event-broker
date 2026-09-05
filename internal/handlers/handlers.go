package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/broker"
)

type Handler struct {
	Broker          *broker.Broker
	tailBroadcaster *tailBroadcaster
}

func New(b *broker.Broker) *Handler {
	return &Handler{Broker: b, tailBroadcaster: newTailBroadcaster()}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func statusForBrokerErr(err error) int {
	switch {
	case errors.Is(err, broker.ErrTopicNotFound):
		return http.StatusNotFound
	case errors.Is(err, broker.ErrTopicAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, broker.ErrPartitionOutOfRange):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

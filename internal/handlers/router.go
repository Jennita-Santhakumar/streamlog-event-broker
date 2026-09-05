package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(h *Handler, corsOrigins string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware(corsOrigins))

	r.Get("/health", h.Health)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Route("/topics", func(r chi.Router) {
			r.Post("/", h.CreateTopic)
			r.Get("/", h.ListTopics)
			r.Delete("/{topic}", h.DeleteTopic)
		})

		r.Post("/events/{topic}", h.PublishEvent)
		r.Get("/events/{topic}/ws", h.TailEvents)

		r.Route("/consumers/{group}", func(r chi.Router) {
			r.Get("/events/{topic}", h.PullEvents)
			r.Post("/commit", h.CommitOffset)
		})

		r.Post("/dlq", h.ReportDLQ)
		r.Get("/dlq", h.ListDLQ)

		r.Get("/lag", h.ConsumerLag)
	})

	return r
}

func corsMiddleware(origins string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", origins)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_messages_published_total",
		Help: "Total number of messages successfully published.",
	}, []string{"topic", "partition"})

	MessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_messages_consumed_total",
		Help: "Total number of messages returned to consumers.",
	}, []string{"topic", "partition"})

	PublishLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "broker_publish_latency_seconds",
		Help:    "Latency of publish requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	ConsumeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "broker_consume_latency_seconds",
		Help:    "Latency of consume requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	DLQMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_dlq_messages_total",
		Help: "Total number of messages routed to the dead-letter queue.",
	}, []string{"topic"})
)

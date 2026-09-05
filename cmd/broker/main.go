package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/broker"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/config"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/handlers"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := storage.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}
	defer store.Close()

	b := broker.NewBroker(store, cfg.DataDir, cfg.SegmentMaxBytes, cfg.BrokerID)
	if err := b.LoadExisting(ctx); err != nil {
		log.Fatalf("loading existing topics: %v", err)
	}
	defer b.Close()

	retentionCtx, cancelRetention := context.WithCancel(context.Background())
	defer cancelRetention()
	go b.RunRetentionLoop(retentionCtx, time.Duration(cfg.RetentionCheckIntervalSec)*time.Second)

	h := handlers.New(b)
	router := handlers.NewRouter(h, cfg.CORSOrigins)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}

	go func() {
		log.Printf("broker listening on :%s", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownGraceSec)*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

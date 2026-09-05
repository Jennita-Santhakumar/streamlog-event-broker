package main

import (
	"fmt"
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/joho/godotenv"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/migrations"
)

func main() {
	_ = godotenv.Load()

	if len(os.Args) < 2 {
		log.Fatal("usage: migrate <up|down>")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		log.Fatalf("loading embedded migrations: %v", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dbURL)
	if err != nil {
		log.Fatalf("migrate init: %v", err)
	}

	switch os.Args[1] {
	case "up":
		err = m.Up()
	case "down":
		err = m.Down()
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}

	if err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate %s: %v", os.Args[1], err)
	}

	fmt.Printf("migrate %s: ok\n", os.Args[1])
}

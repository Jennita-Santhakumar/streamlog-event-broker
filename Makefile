.PHONY: lint test test-integration migrate-up migrate-down run-broker compose-up compose-down loadtest

lint:
	golangci-lint run ./...

test:
	go test -race -short ./...

test-integration:
	go test -race -tags=integration ./tests/integration/...

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

run-broker:
	go run ./cmd/broker

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

loadtest:
	go run ./scripts/loadtest

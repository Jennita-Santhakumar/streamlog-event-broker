# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -o migrate ./cmd/migrate

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates wget

WORKDIR /root/

COPY --from=builder /app/broker .
COPY --from=builder /app/migrate .

EXPOSE 8080
CMD ["./broker"]

# Use the official Golang image to build the application
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download 2>/dev/null || true

COPY . .
RUN go build -o /scheduler

# Use a minimal alpine image to run the application
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /scheduler /scheduler

EXPOSE 8080

ENTRYPOINT ["/scheduler"]
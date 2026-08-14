FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /scheduler

FROM alpine:latest

RUN apk --no-cache add ca-certificates

COPY --from=builder /scheduler /scheduler

EXPOSE 8080

ENTRYPOINT ["/scheduler"]

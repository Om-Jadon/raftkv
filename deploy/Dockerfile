# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/raftkvd ./cmd/raftkvd

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget su-exec \
    && adduser -D -H -u 10001 raftkv \
    && mkdir -p /data \
    && chown raftkv:raftkv /data
COPY --from=build /out/raftkvd /usr/local/bin/raftkvd
COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8080 7000
VOLUME /data
HEALTHCHECK --interval=2s --timeout=2s --start-period=2s --retries=20 \
    CMD wget -qO- http://127.0.0.1:8080/status >/dev/null || exit 1
ENTRYPOINT ["/entrypoint.sh"]

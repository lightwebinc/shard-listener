# syntax=docker/dockerfile:1.7
#
# Canonical multi-stage Dockerfile for shard-listener.
# Final image: distroless/static:nonroot.

FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false \
      -ldflags "-s -w -X github.com/lightwebinc/shard-listener/metrics.Version=${VERSION}" \
      -o /out/shard-listener .

FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
COPY --from=builder /out/shard-listener /usr/local/bin/shard-listener
ENTRYPOINT ["/usr/local/bin/shard-listener"]

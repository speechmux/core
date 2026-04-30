# ── Build stage ────────────────────────────────────────────────────────────────
# Build context is the workspace root so that proto/ is available for the
# replace directive: replace github.com/speechmux/proto => ../proto
FROM golang:1.25-alpine AS builder
WORKDIR /workspace

# Cache go mod download separately from source changes.
COPY proto/ ./proto/
COPY core/go.mod core/go.sum ./core/
WORKDIR /workspace/core
RUN go mod download

WORKDIR /workspace
COPY core/ ./core/
WORKDIR /workspace/core
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /speechmux-core ./cmd/speechmux-core

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /speechmux-core /usr/local/bin/speechmux-core

EXPOSE 50051 8090 8091

ENTRYPOINT ["speechmux-core"]
CMD ["--config", "/etc/speechmux/core.yaml", "--plugins", "/etc/speechmux/plugins.yaml"]

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 – Builder
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /build
COPY . .

RUN if [ ! -f go.mod ]; then \
        go mod init github.com/your-org/directory-exporter; \
    fi

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -mod=readonly \
      -ldflags="-s -w -extldflags=-static" \
      -trimpath \
      -o /directory-exporter \
      .

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 – Runtime
# FROM scratch: zero OS, no shell, no package manager, no CVE surface.
# The binary is statically linked — it needs nothing from an OS layer.
# ─────────────────────────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /directory-exporter /directory-exporter

# Run as non-root (UID 65534 = "nobody").
# scratch has no /etc/passwd — set UID numerically so the kernel enforces it.
USER 65534:65534

EXPOSE 9200
ENTRYPOINT ["/directory-exporter"]

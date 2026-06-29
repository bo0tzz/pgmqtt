# syntax=docker/dockerfile:1.25
# Builds a tiny image carrying just the cmd/soak binary. Used by
# scripts/soak.sh to run the traffic generator from inside a kind cluster
# so it hits the broker Service VIP (and thus gets real round-robin
# distribution across replicas), rather than the single-Pod
# `kubectl port-forward svc/...` shortcut.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags "-s -w" -o /out/soak ./cmd/soak

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/soak /soak
USER nonroot:nonroot
ENTRYPOINT ["/soak"]

# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache deps separately for faster rebuilds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/pgmqttd ./cmd/pgmqttd

# Distroless static for a tiny attack surface.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pgmqttd /pgmqttd
USER nonroot:nonroot
EXPOSE 1883 8083
ENTRYPOINT ["/pgmqttd"]

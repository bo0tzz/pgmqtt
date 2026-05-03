.PHONY: build test test-race coverage vet lint helm-lint docker docker-multi smoke clean

BINARY := pgmqttd
PKG := github.com/bo0tzz/pgmqtt
VERSION ?= $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)
IMAGE ?= ghcr.io/bo0tzz/pgmqtt:$(VERSION)

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" \
		-o $(BINARY) ./cmd/pgmqttd

test:
	go test ./... -count=1 -timeout 5m

test-race:
	go test ./... -count=1 -race -timeout 10m

# Outputs a per-package summary (`coverage.txt`) plus the raw profile
# (`coverage.out`). CI uploads both as a build artifact.
coverage:
	go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic -timeout 10m
	go tool cover -func=coverage.out | tee coverage.txt

vet:
	go vet ./...

lint:
	golangci-lint run ./...

helm-lint:
	helm lint deploy/helm/pgmqtt --set database.url='postgres://x/y'
	helm template deploy/helm/pgmqtt --set database.url='postgres://x/y' >/dev/null

docker:
	docker build -t $(IMAGE) --build-arg VERSION=$(VERSION) .

docker-multi:
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) -t $(IMAGE) --push .

smoke:
	bash scripts/smoke.sh

clean:
	rm -f $(BINARY)

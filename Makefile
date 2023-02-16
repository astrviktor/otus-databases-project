BIN := "./bin/creator"
DOCKER_IMG="creator:develop"
CONTAINER_NAME="creator"

GIT_HASH := $(shell git log --format="%h" -n 1)
LDFLAGS := -X main.release="develop" -X main.buildDate=$(shell date -u +%Y-%m-%dT%H:%M:%S) -X main.gitHash=$(GIT_HASH)

build:
	go build -v -o $(BIN) -ldflags "$(LDFLAGS)" "./cmd/main.go"

run: build
	$(BIN) -config ./configs/config_compose.yaml

build-img:
	docker build \
		--build-arg=LDFLAGS="$(LDFLAGS)" \
		-t $(DOCKER_IMG) \
		-f docker/Dockerfile .

run-img: build-img
	docker run -p 8888:8888 $(DOCKER_IMG)

run-detached-img: build-img
	docker run -d --name $(CONTAINER_NAME) -p 8888:8888 $(DOCKER_IMG)

stop-detached-img:
	docker stop $(CONTAINER_NAME)
	docker rm $(CONTAINER_NAME)

test:
	go test -count=100 -race -timeout=5m ./...

install-lint-deps:
	(which golangci-lint > /dev/null) || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.50.1

lint: install-lint-deps
	golangci-lint --config .golangci.yml run ./...

compose-up:
	docker-compose -f deployments/docker-compose.yml up --remove-orphans -d

yandex-tank:
	docker exec -it yandex-tank /bin/bash

compose-down:
	docker-compose -f deployments/docker-compose.yml down -v

docker-volumes:
	docker system df -v | grep "deployments_volume_"


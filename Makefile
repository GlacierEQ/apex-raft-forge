.PHONY: build test vet lint run-node1

build:
	go build -o bin/raftd ./cmd/raftd

test:
	go test -v ./...

vet:
	go vet ./...

lint:
	golangci-lint run

run-node1: build
	./bin/raftd --id node1 --peers node2,node3 --port 8080

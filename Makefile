all: clean build test vet lint

ci: all

clean:
	go clean
	rm -f *.out *.prof *.test
	go mod tidy

build:
	go build ./cmd/server

vet:
	go vet ./...

lint:
	golangci-lint run
#	golint ./...

test:
	go test -bench=. -covermode=count ./...

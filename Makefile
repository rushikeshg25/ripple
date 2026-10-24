.PHONY: build fmt fmt-check vet test check fuzz demo

build:
	go build -trimpath -o bin/ripple ./cmd/ripple

fmt:
	gofmt -w cmd internal

fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }

vet:
	go vet ./...

test:
	go test -race -count=1 ./...

check: fmt-check vet test

fuzz:
	go test ./internal/wire -run='^$$' -fuzz=FuzzParse -fuzztime=5s -parallel=2

demo: build
	python3 scripts/demo.py

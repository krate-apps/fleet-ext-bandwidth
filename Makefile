.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/bandwidth-worker ./cmd/bandwidth-worker

pack:
	bash scripts/pack.sh dist

.PHONY: build test lint clean run loadtest

GO       ?= go
BINARY   := bin/speechmux-core
LOADTEST := bin/loadtest

build:
	$(GO) build -o $(BINARY) ./cmd/speechmux-core

test:
	$(GO) test -race ./...

lint:
	golangci-lint run

clean:
	rm -rf bin/

run: build
	./$(BINARY) --config config/core.yaml --plugins config/plugins.yaml

loadtest:
	$(GO) build -o $(LOADTEST) ./tools/loadtest
	@echo ""
	@echo "loadtest binary ready. Start dummy plugins first, then run:"
	@echo "  make -C .. run-dummy"
	@echo "  ./$(LOADTEST) --sessions 100 --duration 5m"
	@echo ""

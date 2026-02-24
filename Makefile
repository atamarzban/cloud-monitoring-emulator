.PHONY: generate build test run clean docker

GOOGLEAPIS_DIR ?= /tmp/googleapis

generate:
	@if [ ! -d "$(GOOGLEAPIS_DIR)/google/monitoring" ]; then \
		echo "Cloning googleapis..."; \
		git clone --depth 1 --filter=blob:none --sparse https://github.com/googleapis/googleapis.git $(GOOGLEAPIS_DIR) && \
		cd $(GOOGLEAPIS_DIR) && git sparse-checkout set google/monitoring/v3 google/api google/rpc google/type google/longrunning; \
	fi
	cd $(GOOGLEAPIS_DIR) && buf generate --template $(CURDIR)/proto/buf.gen.yaml --path google/monitoring/v3 -o $(CURDIR)

build:
	go build -o bin/emulator ./cmd/emulator

test:
	go test -race -count=1 ./...

run: build
	./bin/emulator

docker:
	docker build -t cloud-monitoring-emulator .

clean:
	rm -rf bin/

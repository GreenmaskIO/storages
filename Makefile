.PHONY: test test-verbose lint clean test-linux test-linux-race

test:
	go test ./...

test-verbose:
	go test -v ./...

lint:
	golangci-lint run ./...

# Run the filesystem-backend tests inside a Linux container (see docker-compose.yml).
# Useful on macOS to exercise real case-sensitive, Unix-permission on-disk behavior.
test-linux:
	docker compose run --rm test

# Same, but with the race detector.
test-linux-race:
	docker compose run --rm test go test -race -count=1 ./ ./directory/... ./inmemory/... ./internal/...

clean:
	rm -rf bin/

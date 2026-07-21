.PHONY: test test-verbose test-race test-integration lint clean test-linux test-linux-race

# The main module: no Docker required anywhere in it.
test:
	go test ./...

test-verbose:
	go test -v ./...

test-race:
	go test -race -count=1 ./...

# End-to-end tests against real servers (MinIO, Azurite, OpenSSH) in containers.
# They live in their own module so testcontainers stays out of the published
# dependency graph, so they are not covered by `make test`.
test-integration:
	cd tests/integration && go test -count=1 -timeout 20m ./...

lint:
	golangci-lint run ./...
	cd tests/integration && golangci-lint run ./...

# Run the main module's tests inside a Linux container (see docker-compose.yml).
# Useful on macOS to exercise real case-sensitive, Unix-permission on-disk behavior.
test-linux:
	docker compose run --rm test

# Same, but with the race detector.
test-linux-race:
	docker compose run --rm test go test -race -count=1 ./...

clean:
	rm -rf bin/

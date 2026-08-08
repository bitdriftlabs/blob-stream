.PHONY: build
build:
	ci/setup.sh
	SKIP_PROTO_GEN=1 cargo build --workspace

.PHONY: test
test:
	DOCKER_COMPOSE_UP=1 ci/setup.sh
	SKIP_PROTO_GEN=1 RUST_LOG=bd_panic=error RUST_BACKTRACE=1 cargo nextest run --workspace

.PHONY: clippy
clippy:
	ci/setup.sh
	SKIP_PROTO_GEN=1 cargo clippy --workspace --bins --examples --tests -- --no-deps

.PHONY: license
license:
	cargo deny check licenses

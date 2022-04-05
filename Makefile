export RUST_BACKTRACE=1

.PHONY: bench test fuzz check fmt

venv:
	virtualenv -v venv -p python3.7
	. venv/bin/activate && pip install maturin pytest

bench:
	cargo +nightly bench --features=benches,redis-store

test:
	cargo test --all-features
	make venv
	. venv/bin/activate && maturin develop --cargo-extra-args="--all-features"
	. venv/bin/activate && pytest python_tests/

fuzz:
	cargo +nightly fuzz run compare

check:
	cargo +stable check
	cargo +nightly check --all-features
	cd fuzz && cargo +stable check
	cargo +nightly clippy --all-features
	cargo fmt -- --check

fmt:
	cargo fmt
	cd fuzz && cargo fmt

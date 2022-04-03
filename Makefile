export RUST_BACKTRACE=1

.PHONY: test fuzz check fmt

test:
	cargo test --all-features

fuzz:
	cargo +nightly fuzz run compare

check:
	cargo check --all-features
	cd fuzz && cargo +stable check
	cargo clippy --all-features
	cargo fmt -- --check

fmt:
	cargo fmt
	cd fuzz && cargo fmt

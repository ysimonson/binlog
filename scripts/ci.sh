#!/bin/bash

set -ex

rust_variant=$1
os=$2

cargo test --features=redis-store,sqlite-store

if [ "$os" == "ubuntu-latest" ]; then
    if [ "$rust_variant" == "stable" ]; then
        cargo fmt -- --check
        (cd fuzz && cargo check)
    else
        cargo check --all-features
        cargo clippy --all-features
    fi
fi

pip install virtualenv
make venv
source venv/bin/activate
maturin develop --cargo-extra-args="--features=redis-store,sqlite-store,python"
pytest --color=yes python_tests/

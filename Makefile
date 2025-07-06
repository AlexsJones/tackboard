# Makefile for tackboard project

.PHONY: test build run fmt clippy

test:
	cargo test --workspace --all-features

build:
	cargo build --workspace --all-features

run:
	cargo run --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-features -- -D warnings 
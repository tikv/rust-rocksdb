.PHONY: all

all: format build test

build:
	@cargo build 

test:
	@export RUST_BACKTRACE=1 && cargo test -- --nocapture 

format: 
	@cargo fmt -- --write-mode diff | grep -E "Diff .*at line" > /dev/null && cargo fmt -- --write-mode overwrite || exit 0
	@rustfmt --write-mode diff tests/test.rs | grep -E "Diff .*at line" > /dev/null && rustfmt --write-mode overwrite tests/test.rs || exit 0
	@cd librocksdb_sys && cargo fmt -- --write-mode diff | grep -E "Diff .*at line" > /dev/null && cargo fmt -- --write-mode overwrite || exit 0

clean:
	@cargo clean
	@cd librocksdb_sys && cargo clean 
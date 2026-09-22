# Mezaleth

Append-only, exclusively-locked, in-memory-indexed key-value store.
See the module-level doc comment at the top of `src/lib.rs` for the
full design: on-disk format (v1/v2), recovery semantics, locking,
size/resource limits, and durability guarantees.

## Layout

```
Mezaleth/
├── Cargo.toml            # package manifest (see note inside — reconstructed for packaging)
├── src/
│   └── lib.rs             # the library
└── fuzz/                  # cargo-fuzz targets for the record parser / recovery path
    ├── Cargo.toml
    └── fuzz_targets/
        ├── read_record_v1.rs   # fast, in-memory: v1 parser
        ├── read_record_v2.rs   # fast, in-memory: v2 parser (CRC + tombstone invariant)
        └── full_recovery.rs    # slower: full Mezaleth::open() path via a real temp file
```

## Build & test

```
cargo build
cargo test --all-targets --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Fuzzing

Requires `cargo-fuzz`, which needs a nightly toolchain:

```
cargo install cargo-fuzz
rustup toolchain install nightly   # if not already installed

cd fuzz
cargo +nightly fuzz run read_record_v2 -- -max_total_time=300
cargo +nightly fuzz run read_record_v1 -- -max_total_time=300
cargo +nightly fuzz run full_recovery  -- -max_total_time=120
```

The fuzz targets call into `Mezaleth::fuzz_support`, which only exists
when the library is built with `--features fuzzing` (the fuzz crate's
`Cargo.toml` already sets this via its path dependency).

If a fuzz run finds a crashing input, it will be saved under
`fuzz/artifacts/<target-name>/`. Reproduce it directly with:

```
cargo +nightly fuzz run <target-name> fuzz/artifacts/<target-name>/<crash-file>
```
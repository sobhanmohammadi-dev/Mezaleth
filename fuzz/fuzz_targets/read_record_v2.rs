#![no_main]
#![allow(non_snake_case)] // the library's crate name is capitalized; see its lib.rs

use libfuzzer_sys::fuzz_target;

// Fast, in-memory fuzz target for the v2 (CRC-checked) record parser.
// This is the more important of the two direct-parser targets: it also
// exercises the incremental CRC accumulator and the strict tombstone
// invariant (deleted => value_len == 0 && expires_at == 0) that v1
// doesn't have.
fuzz_target!(|data: &[u8]| {
    Mezaleth::fuzz_support::read_record_v2(data);
});

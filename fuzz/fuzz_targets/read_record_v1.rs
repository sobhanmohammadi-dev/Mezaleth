#![no_main]
#![allow(non_snake_case)] // the library's crate name is capitalized; see its lib.rs

use libfuzzer_sys::fuzz_target;

// Fast, in-memory fuzz target for the v1 (no-CRC) record parser. Never
// touches the filesystem, so this should run at very high iteration
// rates and is the first target to point a fuzzer's time budget at.
fuzz_target!(|data: &[u8]| {
    Mezaleth::fuzz_support::read_record_v1(data);
});

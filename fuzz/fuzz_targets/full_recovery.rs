#![no_main]
#![allow(non_snake_case)] // the library's crate name is capitalized; see its lib.rs

use libfuzzer_sys::fuzz_target;

// Slower, filesystem-backed target: drives the same arbitrary input
// through the real Mezaleth::open() path (header check, the streaming
// record loop, tail truncation, lock acquisition), catching bugs in the
// loop *around* read_record — offset bookkeeping across multiple
// records, truncation boundaries — that the single-record targets
// (read_record_v1/v2) can't see. Run this with a smaller time budget
// than the two in-memory targets; each iteration does real file I/O.
fuzz_target!(|data: &[u8]| {
    Mezaleth::fuzz_support::full_recovery(data);
});

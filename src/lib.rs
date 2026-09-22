//! Mezaleth: append-only, exclusively-locked, in-memory-indexed key-value store.
//!
//! # State machine
//!
//! ```text
//! Open -> Failed   (on any I/O error during write/flush/sync/compact-install)
//! Open -> Closed   (on successful close())
//! Failed -> Closed (close() is still allowed from Failed, to release resources)
//! ```
//!
//! Once `Failed`, no further mutation is attempted: the writer's on-disk
//! state after a partial/failed write is unknown, so continuing to write
//! could interleave garbage with a subsequent legitimate record. `set`,
//! `set_with_ttl`, `remove`, and `compact` all return
//! `MezError::FailedStorage` once in this state. The only way out is to
//! `close()` and reopen the file fresh (which re-runs recovery/truncation
//! against whatever is actually on disk).
//!
//! # Locking
//!
//! `open()` / `open_or_create()` / `create_new()` / `create_truncate()`
//! all acquire an exclusive advisory lock (flock on Unix, LockFileEx on
//! Windows) on a `<path>.lock` sidecar file and hold it for the
//! instance's lifetime, released on drop. A second attempt to open the
//! same database — in this process or another — fails with
//! `MezError::AlreadyOpen`. This is advisory locking: it protects
//! cooperating processes/instances, not against a process that ignores
//! locks entirely. On platforms other than Unix/Windows no lock is taken
//! (see `MezError::AlreadyOpen` docs) — this is stated honestly rather
//! than silently providing no protection while appearing to.
//!
//! # On-disk format
//!
//! ```text
//! [magic: 8 bytes "MEZALETH"]
//! [version: u8]
//! <records...>
//! ```
//!
//! ## Version 1 (legacy, read-only)
//! ```text
//! [key_len: u32 LE][key][value_len: u32 LE][value][expires_at: u64 LE][flags: u8]
//! ```
//! A v1 file is opened with **no writer at all** — not merely a checked
//! convention, the file handle itself is read-only. `set`/`set_with_ttl`/
//! `remove` return `MezError::LegacyReadOnly`. Call [`Mezaleth::compact`]
//! to upgrade to v2 in place, after which the same instance becomes
//! writable.
//!
//! ## Version 2 (current, default for new writes)
//! ```text
//! [key_len: u32 LE][key][value_len: u32 LE][value][expires_at: u64 LE][flags: u8][crc32: u32 LE]
//! ```
//! `crc32` is CRC-32 (IEEE 802.3 polynomial) computed incrementally over
//! every preceding field, in order, using a genuinely incremental
//! accumulator — no second buffer holding the whole record is ever
//! allocated merely to check the checksum. This is an **integrity**
//! check against accidental corruption; it is **not** authentication.
//!
//! ### Tombstone invariant
//!
//! A record with `flags & FLAG_DELETED != 0` must have `value_len == 0`
//! and `expires_at == 0` — the encoder never produces anything else for a
//! tombstone. A structurally complete record that violates this is
//! rejected with `MezError::InvalidFormat` rather than silently
//! normalized, since accepting it would mean trusting fields the format
//! doesn't actually define a meaning for.
//!
//! # Size and resource limits
//!
//! [`Limits`] bounds, enforced on both write and recovery (v1 and v2
//! alike):
//! - `max_key_len` / `max_value_len` / `max_record_len` — per-record caps.
//!   A length is only ever reported as `RecordTooLarge` once it has
//!   already been established that the file has enough remaining bytes
//!   to structurally contain it; a length that can't possibly fit what's
//!   left in the file is always treated as an **incomplete tail**
//!   instead, since the two are genuinely indistinguishable from the
//!   length field alone in that case (see "Recovery semantics").
//! - `max_entries` — caps the number of keys held in the in-memory index
//!   (including expired-but-not-yet-purged ones). Checked before a *new*
//!   key is inserted; overwriting an existing key is always allowed.
//! - `max_index_bytes` — a heuristic cap (`key.len() + value.len() + 48`
//!   summed per entry, 48 being an approximate per-entry overhead
//!   estimate for the `HashMap`/`Vec` allocations) on total in-memory
//!   index size. Not an exact accounting of allocator overhead.
//! - `max_log_bytes` — caps the on-disk log size. Checked *before* a
//!   record is appended, so a write that would exceed the budget is
//!   rejected with `MezError::NeedsCompaction` without any bytes being
//!   written; the log size tracked for this check is an in-memory
//!   running total updated on every successful append and reset to the
//!   compacted size after `compact()`, not re-derived from the
//!   filesystem on every write.
//!
//! All four are `Option`/optional and unbounded by default (previous
//! behavior). `MezError::NeedsCompaction` signals "compact or raise the
//! limit," distinct from `RecordTooLarge` (a single record is
//! individually too large) and from `FailedStorage` (an I/O error, not a
//! policy decision).
//!
//! # Recovery semantics
//!
//! Recovery streams the log through a bounded `BufReader`, operating on
//! the *same opened file handle* used to stat the file length (no
//! separate `fs::metadata(path)` + `File::open(path)` TOCTOU window).
//!
//! Peak memory is O(live index size + the currently parsed record + a
//! small bounded parser buffer) — not O(file size), and not independent
//! of the current record's own size, since that record's key/value
//! bytes must exist in memory to be inserted into the index.
//!
//! Three outcomes:
//! - **Incomplete tail**: file ends before a record is structurally
//!   complete, or a length claims more bytes than remain in the file.
//!   Truncated back to the last complete, valid record boundary.
//! - **Size-limit violation**: a length that fits within the file's
//!   remaining bytes but exceeds configured [`Limits`].
//!   `MezError::RecordTooLarge`.
//! - **Corruption**: a structurally complete v2 record whose checksum
//!   doesn't match, or a structurally invalid combination (bad flags,
//!   invalid tombstone). Returns `MezError::CorruptedRecord { offset }`
//!   (checksum) or `MezError::InvalidFormat` (structural) and is never
//!   silently dropped or truncated.
//!
//! On `open()`, any leftover compaction temp files matching this exact
//! database's naming scheme in the same directory are removed as
//! best-effort crash-recovery hygiene (see
//! `cleanup_orphaned_temp_files`); this can only affect files left by a
//! *previous, no-longer-running* instance, since a live compacting
//! instance still holds the exclusive lock this `open()` had to acquire
//! first.
//!
//! # Durability model
//!
//! - `flush()`: flushes Mezaleth's own `BufWriter` only. No OS
//!   durability. On failure, transitions to `Failed`.
//! - `sync()`: flush + `sync_data()`. Requests OS persistence; not a
//!   universal guarantee on every storage stack. On failure, `Failed`.
//! - `close()`: flushes and drops the writer, surfacing I/O errors
//!   instead of relying on `Drop`. On success, transitions to `Closed`
//!   (further writes return `MezError::Closed`). On failure, transitions
//!   to `Failed` but the writer is retained so buffered data isn't
//!   silently discarded; the caller should still treat the instance as
//!   unusable for further mutation and reopen a fresh handle. Idempotent
//!   once `Closed`.
//! - `Drop`: if a `Mezaleth` is dropped without an explicit `close()`,
//!   its buffered writer is given a best-effort final flush. Like any
//!   `Drop` impl, this cannot return a `Result`, so a failure here is
//!   unobservable — call `close()`/`flush()`/`sync()` explicitly
//!   beforehand if you need to know whether the final flush succeeded.
//! - `compact()`: see "Compaction state machine" below.
//!
//! # Compaction state machine
//!
//! Every live entry is validated against V2-encoded [`Limits`] (i.e.
//! including the CRC overhead) **before any file is created**, which is
//! what prevents a V1 record — valid under V1's smaller fixed overhead —
//! from silently producing an over-limit V2 record on migration.
//!
//! ```text
//! validate all entries against V2 limits
//!     -> create unique temp file (create_new, same directory)
//!     -> write header + all live V2 records
//!     -> restore destination's Unix permission bits onto temp file
//!     -> flush + sync_all() temp file
//!   [pre-install failure => CompactionFailedBeforeInstall; original file untouched]
//!     -> atomically replace destination with temp file
//!   [replace failure => CompactionFailedBeforeInstall; original file still at path]
//!     -> (point of no return: destination now holds the new V2 file)
//!     -> reopen destination as the new append writer
//!   [reopen failure => CompactionInstalledButIncomplete; data is safe on
//!    disk but this instance moves to Failed rather than silently
//!    committing in-memory state that might not match; reopen the path
//!    fresh to pick up the installed file]
//!     -> commit in-memory format_version = V2Current, reset log-size
//!        accounting to the compacted size
//! ```
//!
//! `compact()` is O(live database size) and is a blocking, exclusive
//! operation for the instance (no concurrent access is possible anyway,
//! since it takes `&mut self`); it is invoked manually — this
//! implementation does not auto-trigger compaction on `max_log_bytes`,
//! it only refuses further writes with `NeedsCompaction` once the budget
//! would be exceeded.
//!
//! # Ownership model
//!
//! Single-process-instance-per-lock, no *internal* concurrency beyond
//! the exclusive lock. Wrap in a `Mutex`/`RwLock` externally for
//! multithreaded use of one instance within a process.
//!
//! # Hasher
//!
//! The in-memory index uses a small FxHash-style non-cryptographic
//! hasher, seeded once per process from a mix of time/PID/stack-address
//! entropy (see `process_seed`) rather than a fixed constant, so a
//! precomputed collision set doesn't carry over across process restarts.
//! It is still not appropriate for keys chosen by an adversary who can
//! observe this process's hash outputs or timing.
//!
//! # TTL semantics
//!
//! - Expiration uses **absolute Unix time, one-second resolution**, via
//!   `SystemTime::now()`. It is wall-clock, not monotonic: a clock
//!   rollback can make an entry live longer than intended, and a clock
//!   jump forward can expire entries early. This is documented rather
//!   than silently assumed away, since persisted TTLs necessarily rely
//!   on wall-clock time to remain meaningful across restarts.
//! - `set_with_ttl(key, value, 0)` is defined as `remove(key)` — no
//!   value record is ever written to the log for a zero-TTL call.
//! - `len()` returns the number of **indexed** keys, including any that
//!   are expired but not yet purged by `get()`'s lazy check or an
//!   explicit `purge_expired()` call — it is index size, not live-key
//!   count.
//!
//! # Platform support
//!
//! Locking and durable atomic replacement are implemented for Unix and
//! Windows. On other platforms, replacement falls back to a
//! non-atomic remove-then-rename (weaker durability, documented as such,
//! never claimed to be atomic), and no file lock is taken at all
//! (`try_lock` is a no-op that always succeeds) — multi-instance safety
//! on such platforms is the caller's responsibility.
//!
//! # Known remaining limitations
//!
//! - The write path buffers a full record in `record_buf` before
//!   writing; for very large values this means caller-owned value +
//!   `record_buf` + `EntryRec.value` can all exist briefly. Not
//!   streaming. Use `max_value_len`/`max_record_len` if this matters.
//! - `RecordTooLarge` carries structured detail (see
//!   [`SizeLimitViolation`]: `kind`, `actual`, `limit`, and `offset` when
//!   known), but other variants (`InvalidFormat`, `CorruptedRecord`'s
//!   reason) are not yet similarly broken down beyond what's shown above.
//! - Fault injection for write/flush/sync failures exists (see the
//!   `fault_injection` test-only module) as thread-local one-shot flags
//!   checked immediately before the real I/O call, rather than a
//!   generic mockable `Write` seam threaded through the public type —
//!   simpler, but it can only inject failures at those three specific
//!   points, not arbitrary partial-write byte counts.
//! - `max_index_bytes` accounting is a heuristic, not exact allocator
//!   accounting.

// The package name `Mezaleth` (capitalized) trips rustc's crate-name
// snake_case lint under `-D warnings`. Renaming the Cargo.toml package
// name to lowercase is the cleaner long-term fix; allowed here so this
// file alone can satisfy `cargo clippy -D warnings` regardless of the
// package name in use.
#![allow(non_snake_case)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 8] = b"MEZALETH";
const VERSION_1: u8 = 1;
const VERSION_2: u8 = 2;
const CURRENT_VERSION: u8 = VERSION_2;
const FLAG_DELETED: u8 = 1;
const KNOWN_FLAGS: u8 = FLAG_DELETED;

const BUFFER_SIZE: usize = 64 * 1024;
const FIXED_FIELDS_LEN_V1: u64 = 4 + 4 + 8 + 1;
const FIXED_FIELDS_LEN_V2: u64 = FIXED_FIELDS_LEN_V1 + 4;

const ESTIMATED_AVG_RECORD_LEN: usize = 64;
const MAX_ESTIMATED_CAPACITY: usize = 1_000_000;
const TEMP_FILE_CREATE_ATTEMPTS: u32 = 16;

/// Approximate per-entry overhead (bytes) added on top of key+value
/// length when estimating in-memory index size for `max_index_bytes`.
/// A heuristic, not exact allocator accounting.
const APPROX_ENTRY_OVERHEAD: usize = 48;

/// Instance-lifetime state. See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageState {
    Open,
    /// An I/O error occurred during a write-adjacent operation; the
    /// on-disk state is not known to be clean. No further mutation is
    /// attempted until the store is closed and reopened.
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageVersion {
    V1Legacy,
    V2Current,
}

/// Configurable size and resource policy. See module docs for what each
/// field bounds and when it's checked.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_key_len: u32,
    pub max_value_len: u32,
    pub max_record_len: u64,
    pub max_entries: Option<usize>,
    pub max_index_bytes: Option<u64>,
    pub max_log_bytes: Option<u64>,
}

impl Limits {
    pub const fn new(max_key_len: u32, max_value_len: u32) -> Self {
        Self {
            max_key_len,
            max_value_len,
            max_record_len: u64::MAX,
            max_entries: None,
            max_index_bytes: None,
            max_log_bytes: None,
        }
    }

    pub const fn with_max_record_len(mut self, max_record_len: u64) -> Self {
        self.max_record_len = max_record_len;
        self
    }

    pub const fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = Some(max_entries);
        self
    }

    pub const fn with_max_index_bytes(mut self, max_index_bytes: u64) -> Self {
        self.max_index_bytes = Some(max_index_bytes);
        self
    }

    pub const fn with_max_log_bytes(mut self, max_log_bytes: u64) -> Self {
        self.max_log_bytes = Some(max_log_bytes);
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_key_len: 1024 * 1024,
            max_value_len: 64 * 1024 * 1024,
            max_record_len: u64::MAX,
            max_entries: None,
            max_index_bytes: None,
            max_log_bytes: None,
        }
    }
}

/// Which configured bound a [`SizeLimitViolation`] exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitKind {
    KeyLen,
    ValueLen,
    RecordLen,
}

/// Structured detail for a `MezError::RecordTooLarge`, so a caller
/// diagnosing a corrupted or misconfigured database doesn't have to
/// re-derive which bound was hit or by how much.
#[derive(Debug, Clone, Copy)]
pub struct SizeLimitViolation {
    pub kind: LimitKind,
    /// The length (or, for `RecordLen`, an arithmetic-overflow sentinel
    /// of `u64::MAX` when the true value couldn't even be computed
    /// without overflowing) that exceeded `limit`.
    pub actual: u64,
    pub limit: u64,
    /// Byte offset of the record in the file, when known — set during
    /// recovery, `None` for a violation detected on a fresh `set()` call
    /// (there is no on-disk offset yet for a record that was never
    /// written).
    pub offset: Option<u64>,
}

impl fmt::Display for SizeLimitViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = match self.kind {
            LimitKind::KeyLen => "key length",
            LimitKind::ValueLen => "value length",
            LimitKind::RecordLen => "total record length",
        };
        match self.offset {
            Some(offset) => write!(
                f,
                "{field} {} exceeds configured limit {} (record at byte offset {offset})",
                self.actual, self.limit
            ),
            None => write!(f, "{field} {} exceeds configured limit {}", self.actual, self.limit),
        }
    }
}

#[derive(Debug)]
pub enum MezError {
    Io(io::Error),
    InvalidFormat,
    CorruptedRecord { offset: u64 },
    RecordTooLarge(SizeLimitViolation),
    /// The writer is unavailable — never opened for writing (v1), or the
    /// store has moved to `Closed`.
    Closed,
    /// The store was opened from a v1 file and hasn't been compacted yet.
    LegacyReadOnly,
    /// The store previously hit an I/O error during a write-adjacent
    /// operation and is refusing further mutation. Close and reopen.
    FailedStorage,
    /// Another `Mezaleth` instance (in this process or another) already
    /// holds the exclusive lock on this database. Not raised on
    /// platforms where no lock implementation is available (see module
    /// "Platform support" docs).
    AlreadyOpen,
    /// A write was rejected because it would exceed a configured
    /// resource limit (`max_entries`, `max_index_bytes`, or
    /// `max_log_bytes`). Compact to reduce log size, or raise the limit.
    /// Nothing was written to disk or the in-memory index for a call
    /// that returns this.
    NeedsCompaction,
    /// Compaction failed before the destination file was replaced. The
    /// original file at the database path is untouched.
    CompactionFailedBeforeInstall(io::Error),
    /// Compaction's atomic replacement of the destination **succeeded**,
    /// but a step after that (permission restore aside — that happens
    /// pre-install — specifically reopening the writer) failed. The
    /// on-disk file at the database path is the new V2 file; this
    /// instance is now `Failed` and should be closed and reopened to
    /// pick the installed file up cleanly.
    CompactionInstalledButIncomplete(io::Error),
}

impl fmt::Display for MezError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::InvalidFormat => write!(f, "invalid Mezaleth storage format"),
            Self::CorruptedRecord { offset } => {
                write!(f, "corrupted record at byte offset {offset}: checksum mismatch")
            }
            Self::RecordTooLarge(violation) => write!(f, "record too large: {violation}"),
            Self::Closed => write!(f, "storage writer is unavailable (closed)"),
            Self::LegacyReadOnly => {
                write!(f, "storage file is legacy version 1 and is read-only; compact() to upgrade")
            }
            Self::FailedStorage => {
                write!(f, "storage is in a failed state after a prior I/O error; close and reopen")
            }
            Self::AlreadyOpen => write!(f, "storage file is already open (locked) by another instance"),
            Self::NeedsCompaction => {
                write!(f, "write rejected: would exceed a configured resource limit; compact() or raise the limit")
            }
            Self::CompactionFailedBeforeInstall(err) => {
                write!(f, "compaction failed before install, original file untouched: {err}")
            }
            Self::CompactionInstalledButIncomplete(err) => {
                write!(f, "compaction installed the new file but a post-install step failed: {err}")
            }
        }
    }
}

impl Error for MezError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::CompactionFailedBeforeInstall(err) => Some(err),
            Self::CompactionInstalledButIncomplete(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for MezError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, MezError>;

struct EntryRec {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

/// Fast, non-cryptographic hasher for trusted, short byte-string keys,
/// seeded once per process (see `process_seed`). See module docs for its
/// threat model.
pub struct FxHasher {
    hash: u64,
    /// Cached once at construction time (see the manual `Default` impl
    /// below), rather than re-resolved from `process_seed()`'s
    /// `OnceLock` on every `write`/`write_u8`/`write_u64` call. A new
    /// `FxHasher` is built once per hashed value (via
    /// `BuildHasherDefault::build_hasher()`), so caching here turns what
    /// would otherwise be a `OnceLock` check on every hashed byte chunk
    /// into a single plain field read for that value's entire hash.
    seed: u64,
}

impl Default for FxHasher {
    #[inline]
    fn default() -> Self {
        Self { hash: 0, seed: process_seed() }
    }
}

fn process_seed() -> u64 {
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SEED.get_or_init(|| {
        // Mix process-specific, time-specific, and address-space-specific
        // entropy. Not cryptographically secure — the goal is defeating a
        // precomputed collision set across process restarts, not
        // confidentiality against an attacker who can already observe
        // this process's internals.
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let pid = std::process::id() as u64;
        let stack_addr = &t as *const _ as u64;
        t.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ pid.wrapping_mul(0xBF58_476D_1CE4_E5B9)
            ^ stack_addr.rotate_left(17)
            ^ 0x51_7c_c1_b7_27_22_0a_95
    })
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        let seed = self.seed;
        while bytes.len() >= 8 {
            let word = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
            self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(seed);
            bytes = &bytes[8..];
        }
        if bytes.len() >= 4 {
            let word = u32::from_ne_bytes(bytes[..4].try_into().unwrap()) as u64;
            self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(seed);
            bytes = &bytes[4..];
        }
        for &b in bytes {
            self.hash = (self.hash.rotate_left(5) ^ b as u64).wrapping_mul(seed);
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.hash = (self.hash.rotate_left(5) ^ i as u64).wrapping_mul(self.seed);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(self.seed);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

type FxBuildHasher = BuildHasherDefault<FxHasher>;
type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// Holds the exclusive advisory lock on the data file for the instance's
/// lifetime. Released when dropped.
struct FileLock {
    #[cfg(any(unix, windows))]
    _file: File,
    #[cfg(not(any(unix, windows)))]
    _unsupported: (),
}

#[cfg(unix)]
mod lock_impl {
    use super::*;
    use std::os::unix::io::AsRawFd;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;

    pub fn try_lock(path: &Path) -> Result<FileLock> {
        // A dedicated `<path>.lock` sidecar rather than locking the data
        // file directly: this keeps the lock's identity stable and
        // independent of the data file's own open/rename lifecycle
        // (compact() replaces the underlying inode of the data file, but
        // never touches the lock file).
        let lock_path = lock_file_path(path);
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&lock_path)?;

        // SAFETY: fd is a valid, open file descriptor owned by `file` for
        // the duration of this call; flock does not retain it beyond the
        // call.
        let ret = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if ret != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(MezError::AlreadyOpen);
            }
            return Err(MezError::Io(err));
        }
        Ok(FileLock { _file: file })
    }

    fn lock_file_path(path: &Path) -> PathBuf {
        let mut s = path.as_os_str().to_owned();
        s.push(".lock");
        PathBuf::from(s)
    }
}

#[cfg(windows)]
mod lock_impl {
    use super::*;
    use std::os::windows::io::AsRawHandle;

    unsafe extern "system" {
        fn LockFileEx(
            hFile: *mut std::ffi::c_void,
            dwFlags: u32,
            dwReserved: u32,
            nNumberOfBytesToLockLow: u32,
            nNumberOfBytesToLockHigh: u32,
            lpOverlapped: *mut std::ffi::c_void,
        ) -> i32;
    }
    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x2;
    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x1;

    // The Win32 error code LockFileEx reports when the region is already
    // locked by someone else under LOCKFILE_FAIL_IMMEDIATELY. This is
    // the *only* code that means "another instance holds this lock" —
    // anything else (permission denied, disk error, etc.) is a genuine
    // I/O failure and must be reported as such rather than
    // misclassified as ordinary contention.
    const ERROR_LOCK_VIOLATION: i32 = 33;

    pub fn try_lock(path: &Path) -> Result<FileLock> {
        let lock_path = lock_file_path(path);
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&lock_path)?;

        let mut overlapped = [0u8; 32]; // OVERLAPPED, zero-initialized is valid.
        // SAFETY: file's handle is valid for the duration of this call;
        // overlapped is a correctly sized, zero-initialized OVERLAPPED
        // structure whose address only needs to be valid for this call,
        // since LOCKFILE_FAIL_IMMEDIATELY makes this a synchronous,
        // non-queued attempt.
        let ok = unsafe {
            LockFileEx(
                file.as_raw_handle() as *mut _,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                overlapped.as_mut_ptr() as *mut _,
            )
        };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // Only ERROR_LOCK_VIOLATION means genuine contention from
            // another lock holder; anything else (permission denied, a
            // disk error, an invalid handle, ...) is a real failure and
            // must not be swallowed into a misleading AlreadyOpen.
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION) {
                return Err(MezError::AlreadyOpen);
            }
            return Err(MezError::Io(err));
        }
        Ok(FileLock { _file: file })
    }

    fn lock_file_path(path: &Path) -> PathBuf {
        let mut s = path.as_os_str().to_owned();
        s.push(".lock");
        PathBuf::from(s)
    }
}

#[cfg(not(any(unix, windows)))]
mod lock_impl {
    use super::*;
    pub fn try_lock(_path: &Path) -> Result<FileLock> {
        // No portable advisory lock available on this platform; do not
        // pretend to provide one (see module "Platform support" docs).
        Ok(FileLock { _unsupported: () })
    }
}

/// Append-only, single-process, exclusively-locked, in-memory-indexed
/// key-value store. See module docs for state machine, locking, format,
/// limits, and durability details.
pub struct Mezaleth {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    data: FxHashMap<Vec<u8>, EntryRec>,
    record_buf: Vec<u8>,
    limits: Limits,
    format_version: StorageVersion,
    state: StorageState,
    /// Approximate current on-disk log size (header + all records ever
    /// appended since the last compaction), tracked incrementally.
    /// Reset to the compacted file's size after a successful `compact()`.
    approx_log_bytes: u64,
    /// Running total of `current_index_bytes_estimate()`'s accounting
    /// (`key.len() + value.len() + APPROX_ENTRY_OVERHEAD` per entry),
    /// maintained incrementally on every insert/overwrite/remove/purge
    /// rather than recomputed by summing the whole index on every write
    /// — recomputing per-write made a single `set()` call O(n) in the
    /// number of existing entries.
    approx_index_bytes: u64,
    _lock: FileLock,
}

impl Mezaleth {
    /// Create a new storage file, destroying any existing content at
    /// `path`. Explicitly destructive — prefer [`Self::open_or_create`]
    /// or [`Self::create_new`] unless truncation is actually intended.
    pub fn create_truncate<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_truncate_with_limits(path, Limits::default())
    }

    pub fn create_truncate_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = lock_impl::try_lock(&path)?;

        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[CURRENT_VERSION])?;
        file.flush()?;

        let header_len = MAGIC.len() as u64 + 1;
        Ok(Self {
            path,
            writer: Some(BufWriter::with_capacity(BUFFER_SIZE, file)),
            data: FxHashMap::default(),
            record_buf: Vec::with_capacity(256),
            limits,
            format_version: StorageVersion::V2Current,
            state: StorageState::Open,
            approx_log_bytes: header_len,
            approx_index_bytes: 0,
            _lock: lock,
        })
    }

    /// Create a new storage file. Fails with an I/O error
    /// (`ErrorKind::AlreadyExists`) if `path` already exists — the
    /// non-destructive counterpart to [`Self::create_truncate`].
    pub fn create_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_new_with_limits(path, Limits::default())
    }

    pub fn create_new_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = lock_impl::try_lock(&path)?;

        let mut file = OpenOptions::new().create_new(true).write(true).open(&path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[CURRENT_VERSION])?;
        file.flush()?;

        let header_len = MAGIC.len() as u64 + 1;
        Ok(Self {
            path,
            writer: Some(BufWriter::with_capacity(BUFFER_SIZE, file)),
            data: FxHashMap::default(),
            record_buf: Vec::with_capacity(256),
            limits,
            format_version: StorageVersion::V2Current,
            state: StorageState::Open,
            approx_log_bytes: header_len,
            approx_index_bytes: 0,
            _lock: lock,
        })
    }

    /// Open an existing storage file (v1 or v2), acquiring the exclusive
    /// lock first. Fails with `MezError::AlreadyOpen` if another instance
    /// already holds it. A v1 file is opened with no writer at all (see
    /// module docs).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_limits(path, Limits::default())
    }

    pub fn open_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = lock_impl::try_lock(&path)?;

        // Best-effort cleanup of any compaction temp file orphaned by a
        // crashed previous instance. Safe to do now: we hold the
        // exclusive lock, so no live compaction (which would also need
        // this lock) can be in flight.
        cleanup_orphaned_temp_files(&path);

        // Single opened handle used for both the length check and the
        // read that follows: no separate fs::metadata(path) call, so
        // nothing can replace/shrink/grow the file in between.
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        let mut header = [0u8; MAGIC.len() + 1];
        read_exact_or_none(&mut reader, &mut header)?.ok_or(MezError::InvalidFormat)?;
        if &header[..MAGIC.len()] != MAGIC {
            return Err(MezError::InvalidFormat);
        }
        let version = header[MAGIC.len()];
        if version != VERSION_1 && version != VERSION_2 {
            return Err(MezError::InvalidFormat);
        }
        let format_version =
            if version == VERSION_1 { StorageVersion::V1Legacy } else { StorageVersion::V2Current };

        let header_len: u64 = MAGIC.len() as u64 + 1;
        let log_len = file_len.saturating_sub(header_len);
        let estimated_records =
            ((log_len as usize) / ESTIMATED_AVG_RECORD_LEN.max(1)).min(MAX_ESTIMATED_CAPACITY);
        let mut data: FxHashMap<Vec<u8>, EntryRec> =
            FxHashMap::with_capacity_and_hasher(estimated_records, FxBuildHasher::default());

        let now = now_secs();
        let mut offset: u64 = header_len;

        loop {
            let record_start = offset;
            let remaining = file_len.saturating_sub(offset);
            if remaining == 0 {
                break;
            }
            match read_record(&mut reader, version, remaining, &limits)? {
                RecordRead::Incomplete => break,
                RecordRead::Corrupted => return Err(MezError::CorruptedRecord { offset: record_start }),
                RecordRead::TooLarge(mut violation) => {
                    violation.offset = Some(record_start);
                    return Err(MezError::RecordTooLarge(violation));
                }
                RecordRead::Ok { consumed, key, value, expires_raw, deleted } => {
                    offset = offset.checked_add(consumed).ok_or(MezError::InvalidFormat)?;
                    if deleted {
                        data.remove(key.as_slice());
                        continue;
                    }
                    let expires_at = if expires_raw == 0 { None } else { Some(expires_raw) };
                    if expires_at.is_some_and(|e| e <= now) {
                        data.remove(key.as_slice());
                        continue;
                    }
                    match data.get_mut(key.as_slice()) {
                        Some(entry) => {
                            entry.value = value;
                            entry.expires_at = expires_at;
                        }
                        None => {
                            data.insert(key, EntryRec { value, expires_at });
                        }
                    }
                }
            }
        }

        let valid_size = offset;
        drop(reader);

        if valid_size < file_len {
            let trunc_file = OpenOptions::new().write(true).open(&path)?;
            debug_assert!(valid_size <= trunc_file.metadata()?.len());
            trunc_file.set_len(valid_size)?;
            drop(trunc_file);
        }

        // V1 files get a genuinely read-only situation: no writer at
        // all. Only compact() creates the write path needed to upgrade
        // to V2.
        let writer = if format_version == StorageVersion::V1Legacy {
            None
        } else {
            let append_file = OpenOptions::new().read(true).append(true).open(&path)?;
            Some(BufWriter::with_capacity(BUFFER_SIZE, append_file))
        };

        // One-time O(n) sum at startup only — recovery already touches
        // every record to build `data`, so this piggybacks on work
        // that's unavoidable anyway. From here on, `approx_index_bytes`
        // is maintained incrementally (see `set_inner`/`remove_bytes`),
        // never recomputed by re-summing the whole index.
        let approx_index_bytes = data
            .iter()
            .map(|(k, e)| (k.len().saturating_add(e.value.len()).saturating_add(APPROX_ENTRY_OVERHEAD)) as u64)
            .sum();

        Ok(Self {
            path,
            writer,
            data,
            record_buf: Vec::with_capacity(256),
            limits,
            format_version,
            state: StorageState::Open,
            approx_log_bytes: valid_size,
            approx_index_bytes,
            _lock: lock,
        })
    }

    /// Open if it exists, otherwise create — atomically: always attempts
    /// an exclusive `create_new` first and only falls back to `open()`
    /// if that loses to a concurrent creator, so no caller can ever
    /// truncate data another caller just created.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_or_create_with_limits(path, Limits::default())
    }

    pub fn open_or_create_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self> {
        match Self::create_new_with_limits(&path, limits) {
            Ok(store) => Ok(store),
            Err(MezError::Io(err)) if err.kind() == io::ErrorKind::AlreadyExists => {
                Self::open_with_limits(path, limits)
            }
            Err(err) => Err(err),
        }
    }

    #[deprecated(note = "use Mezaleth::create_truncate or ::open_or_create instead")]
    pub fn newstorage<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_truncate(path)
    }

    /// Backward-compatible alias for the historical "create, truncating
    /// if present" behavior. Routes to the explicitly destructive
    /// constructor so existing call sites keep their old semantics
    /// rather than silently changing.
    #[deprecated(note = "ambiguous name — use create_truncate() or create_new() explicitly")]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_truncate(path)
    }

    pub fn is_legacy_read_only(&self) -> bool {
        self.format_version == StorageVersion::V1Legacy
    }

    pub fn is_failed(&self) -> bool {
        self.state == StorageState::Failed
    }

    fn ensure_writable(&self) -> Result<()> {
        match self.state {
            StorageState::Failed => return Err(MezError::FailedStorage),
            StorageState::Closed => return Err(MezError::Closed),
            StorageState::Open => {}
        }
        if self.format_version == StorageVersion::V1Legacy {
            return Err(MezError::LegacyReadOnly);
        }
        Ok(())
    }

    /// Set a key without expiration.
    pub fn set<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.set_inner(key.as_ref(), value.as_ref(), None)
    }

    /// Set a key with a TTL in seconds. `ttl_secs == 0` is defined as
    /// `remove(key)` — see module TTL-semantics docs.
    pub fn set_with_ttl<K, V>(&mut self, key: K, value: V, ttl_secs: u64) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.set_inner(key.as_ref(), value.as_ref(), Some(ttl_secs))
    }

    fn set_inner(&mut self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<()> {
        self.ensure_writable()?;

        // TTL = 0 is deletion; never write a value record for it.
        if ttl_secs == Some(0) {
            return self.remove_bytes(key);
        }

        // Key/value/record size limits are enforced inside
        // append_record() itself, right before the actual write — a
        // separate check_limits_for_version() call here would just
        // redo the same validation on every set() for no extra safety.
        self.check_resource_limits(key, value)?;

        let expires_at = ttl_secs.map(|ttl| now_secs().saturating_add(ttl));

        // Precompute the index-byte-accounting delta before mutating
        // anything, so it can be applied in one step once the write (and
        // the data mutation it guards) has actually succeeded.
        let old_entry_bytes: u64 = self
            .data
            .get(key)
            .map(|e| (key.len().saturating_add(e.value.len()).saturating_add(APPROX_ENTRY_OVERHEAD)) as u64)
            .unwrap_or(0);
        let new_entry_bytes: u64 =
            (key.len().saturating_add(value.len()).saturating_add(APPROX_ENTRY_OVERHEAD)) as u64;

        self.append_record(key, value, expires_at, false)?;

        if let Some(entry) = self.data.get_mut(key) {
            entry.value.clear();
            entry.value.extend_from_slice(value);
            entry.expires_at = expires_at;
        } else {
            self.data.insert(key.to_vec(), EntryRec { value: value.to_vec(), expires_at });
        }
        self.approx_index_bytes =
            self.approx_index_bytes.saturating_sub(old_entry_bytes).saturating_add(new_entry_bytes);

        Ok(())
    }

    /// Validate key/value/total-record length against configured
    /// [`Limits`] **as encoded for `version`** — v2's CRC overhead is
    /// included when `version == VERSION_2`.
    fn check_limits_for_version(&self, key_len: usize, value_len: usize, version: u8) -> Result<()> {
        if key_len > self.limits.max_key_len as usize {
            return Err(MezError::RecordTooLarge(SizeLimitViolation {
                kind: LimitKind::KeyLen,
                actual: key_len as u64,
                limit: self.limits.max_key_len as u64,
                offset: None,
            }));
        }
        if value_len > self.limits.max_value_len as usize {
            return Err(MezError::RecordTooLarge(SizeLimitViolation {
                kind: LimitKind::ValueLen,
                actual: value_len as u64,
                limit: self.limits.max_value_len as u64,
                offset: None,
            }));
        }
        if key_len > u32::MAX as usize {
            return Err(MezError::RecordTooLarge(SizeLimitViolation {
                kind: LimitKind::KeyLen,
                actual: key_len as u64,
                limit: u32::MAX as u64,
                offset: None,
            }));
        }
        if value_len > u32::MAX as usize {
            return Err(MezError::RecordTooLarge(SizeLimitViolation {
                kind: LimitKind::ValueLen,
                actual: value_len as u64,
                limit: u32::MAX as u64,
                offset: None,
            }));
        }
        let fixed = if version == VERSION_1 { FIXED_FIELDS_LEN_V1 } else { FIXED_FIELDS_LEN_V2 };
        let total: Option<u64> =
            fixed.checked_add(key_len as u64).and_then(|v| v.checked_add(value_len as u64));
        match total {
            None => {
                return Err(MezError::RecordTooLarge(SizeLimitViolation {
                    kind: LimitKind::RecordLen,
                    actual: u64::MAX,
                    limit: self.limits.max_record_len,
                    offset: None,
                }))
            }
            Some(total) if total > self.limits.max_record_len => {
                return Err(MezError::RecordTooLarge(SizeLimitViolation {
                    kind: LimitKind::RecordLen,
                    actual: total,
                    limit: self.limits.max_record_len,
                    offset: None,
                }))
            }
            _ => {}
        }
        Ok(())
    }

    /// Check `max_entries` / `max_index_bytes` / `max_log_bytes` against
    /// what a `set()` of `key`/`value` would produce, *before* writing
    /// or mutating the index. Overwriting an existing key never counts
    /// against `max_entries`.
    fn check_resource_limits(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let existing = self.data.get(key);
        let is_new_key = existing.is_none();

        if is_new_key {
            if let Some(max_entries) = self.limits.max_entries {
                if self.data.len() >= max_entries {
                    return Err(MezError::NeedsCompaction);
                }
            }
        }

        if let Some(max_bytes) = self.limits.max_index_bytes {
            let new_entry_bytes = key.len().saturating_add(value.len()).saturating_add(APPROX_ENTRY_OVERHEAD);
            let old_entry_bytes = existing
                .map(|e| key.len().saturating_add(e.value.len()).saturating_add(APPROX_ENTRY_OVERHEAD))
                .unwrap_or(0);
            // O(1): uses the incrementally-maintained running total
            // (`approx_index_bytes`) rather than re-summing every entry
            // in the index on every single `set()` call.
            let projected = self
                .approx_index_bytes
                .saturating_sub(old_entry_bytes as u64)
                .saturating_add(new_entry_bytes as u64);
            if projected > max_bytes {
                return Err(MezError::NeedsCompaction);
            }
        }

        if let Some(max_log) = self.limits.max_log_bytes {
            // Conservative pre-check using the worst-case v2 encoded size
            // (exact size is computed again, cheaply, inside
            // append_record right before the real check/write).
            let approx_record_len = FIXED_FIELDS_LEN_V2
                .saturating_add(key.len() as u64)
                .saturating_add(value.len() as u64);
            if self.approx_log_bytes.saturating_add(approx_record_len) > max_log {
                return Err(MezError::NeedsCompaction);
            }
        }

        Ok(())
    }

    #[deprecated(note = "use set() or set_with_ttl() instead")]
    #[allow(clippy::new_ret_no_self)] // intentional back-compat alias, not the `new -> Self` pattern
    pub fn new<K, V>(&mut self, key: K, value: V, ttl_secs: Option<u64>) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.set_inner(key.as_ref(), value.as_ref(), ttl_secs)
    }

    /// Read a value without allocating. Expired values behave as missing.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
        let entry = self.data.get(key.as_ref())?;
        match entry.expires_at {
            None => Some(entry.value.as_slice()),
            Some(expires_at) => {
                if expires_at <= now_secs() {
                    None
                } else {
                    Some(entry.value.as_slice())
                }
            }
        }
    }

    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.get(key).is_some()
    }

    /// Remove a key. A tombstone is written only if the key currently has
    /// a live (non-expired) in-memory entry.
    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.ensure_writable()?;
        self.remove_bytes(key.as_ref())
    }

    fn remove_bytes(&mut self, key: &[u8]) -> Result<()> {
        let should_write_tombstone = match self.data.get(key) {
            None => false,
            Some(entry) => match entry.expires_at {
                None => true,
                Some(expires_at) => expires_at > now_secs(),
            },
        };
        if should_write_tombstone {
            self.append_record(key, &[], None, true)?;
        }
        if let Some(entry) = self.data.remove(key) {
            let removed_bytes =
                (key.len().saturating_add(entry.value.len()).saturating_add(APPROX_ENTRY_OVERHEAD)) as u64;
            self.approx_index_bytes = self.approx_index_bytes.saturating_sub(removed_bytes);
        }
        Ok(())
    }

    #[deprecated(note = "use remove() instead")]
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.remove(key)
    }

    /// Number of indexed keys, **including** expired-but-not-yet-purged
    /// entries. See module TTL-semantics docs.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Remove expired entries from memory. Returns the number removed.
    pub fn purge_expired(&mut self) -> usize {
        let now = now_secs();
        let before = self.data.len();
        let mut removed_bytes: u64 = 0;
        self.data.retain(|k, entry| {
            let keep = entry.expires_at.is_none_or(|e| e > now);
            if !keep {
                removed_bytes = removed_bytes.saturating_add(
                    (k.len().saturating_add(entry.value.len()).saturating_add(APPROX_ENTRY_OVERHEAD)) as u64,
                );
            }
            keep
        });
        self.approx_index_bytes = self.approx_index_bytes.saturating_sub(removed_bytes);
        before - self.data.len()
    }

    /// Flush Mezaleth's own userspace write buffer. No OS durability.
    /// On failure, transitions to `Failed`.
    pub fn flush(&mut self) -> Result<()> {
        if self.state == StorageState::Failed {
            return Err(MezError::FailedStorage);
        }
        #[cfg(test)]
        if INJECT_FLUSH_ERROR.with(|c| c.replace(false)) {
            self.state = StorageState::Failed;
            return Err(MezError::Io(injected_test_error()));
        }
        let result = self.writer_mut().and_then(|w| w.flush().map_err(MezError::from));
        if result.is_err() {
            self.state = StorageState::Failed;
        }
        result
    }

    /// Flush + `sync_data()`. On failure, transitions to `Failed`.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        #[cfg(test)]
        if INJECT_SYNC_ERROR.with(|c| c.replace(false)) {
            self.state = StorageState::Failed;
            return Err(MezError::Io(injected_test_error()));
        }
        let result = self.writer_mut().and_then(|w| w.get_ref().sync_data().map_err(MezError::from));
        if result.is_err() {
            self.state = StorageState::Failed;
        }
        result
    }

    /// Flush and release the writer, surfacing I/O errors. On success,
    /// transitions to `Closed`. On failure, transitions to `Failed` but
    /// the writer is retained (no buffered data is silently discarded);
    /// the instance should still be treated as unusable for further
    /// mutation. Idempotent once `Closed`.
    pub fn close(&mut self) -> Result<()> {
        if self.state == StorageState::Closed {
            return Ok(());
        }
        if let Some(mut writer) = self.writer.take() {
            if let Err(e) = writer.flush() {
                self.writer = Some(writer);
                self.state = StorageState::Failed;
                return Err(MezError::Io(e));
            }
        }
        self.state = StorageState::Closed;
        Ok(())
    }

    /// Rewrite the storage file using only live, non-expired records, in
    /// v2 format. See module "Compaction state machine" docs for the
    /// pre-install/post-install error distinction. Refuses to run if the
    /// store is `Failed` or `Closed`.
    pub fn compact(&mut self) -> Result<()> {
        match self.state {
            StorageState::Failed => return Err(MezError::FailedStorage),
            StorageState::Closed => return Err(MezError::Closed),
            StorageState::Open => {}
        }

        // A legacy v1 store intentionally has no writer at all (see
        // module docs) — nothing to flush in that case. Calling
        // self.flush() unconditionally would misreport this as
        // MezError::Closed, conflating "never had a writer (v1)" with
        // "writer was explicitly closed."
        if self.writer.is_some() {
            self.flush()?;
        }
        self.purge_expired();

        // Validate every live entry against V2 limits *before* touching
        // disk at all — prevents the V1->V2 migration bug where a record
        // valid under V1's smaller fixed overhead becomes too large once
        // the V2 CRC field is added.
        for (key, entry) in &self.data {
            self.check_limits_for_version(key.len(), entry.value.len(), VERSION_2)?;
        }

        #[cfg(unix)]
        let preserved_mode = fs::metadata(&self.path).ok().map(|m| {
            use std::os::unix::fs::PermissionsExt;
            m.permissions().mode()
        });

        let (tmp_path, tmp_file) = create_unique_temp_file(&self.path).map_err(pre_install_err)?;

        let mut compacted_size: u64 = 0;
        let pre_install: std::result::Result<(), io::Error> = (|| {
            let mut tmp = BufWriter::with_capacity(BUFFER_SIZE, tmp_file);
            tmp.write_all(MAGIC)?;
            tmp.write_all(&[CURRENT_VERSION])?;
            compacted_size += MAGIC.len() as u64 + 1;

            let mut buf = Vec::with_capacity(256);
            for (key, entry) in &self.data {
                buf.clear();
                encode_record_v2(&mut buf, key, &entry.value, entry.expires_at, false)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "record too large"))?;
                tmp.write_all(&buf)?;
                compacted_size += buf.len() as u64;
            }
            tmp.flush()?;
            let tmp_file = tmp.into_inner().map_err(|e| e.into_error())?;

            #[cfg(unix)]
            if let Some(mode) = preserved_mode {
                use std::os::unix::fs::PermissionsExt;
                tmp_file.set_permissions(fs::Permissions::from_mode(mode))?;
            }

            tmp_file.sync_all()?;
            drop(tmp_file);
            Ok(())
        })();

        if let Err(e) = pre_install {
            let _ = fs::remove_file(&tmp_path);
            return Err(MezError::CompactionFailedBeforeInstall(e));
        }

        // --- Point of no return: about to attempt the atomic replace. ---
        self.writer.take();

        if let Err(e) = replace_file_durably(&tmp_path, &self.path) {
            let _ = fs::remove_file(&tmp_path);
            self.state = StorageState::Failed;
            if let Ok(w) = open_append_writer(&self.path) {
                self.writer = Some(w);
            }
            return Err(MezError::CompactionFailedBeforeInstall(e));
        }

        // Replacement succeeded: the new V2 file IS now at self.path
        // regardless of what happens below. Any failure past this point
        // is reported as CompactionInstalledButIncomplete, and we move to
        // Failed rather than silently committing in-memory state that
        // might not match what's actually on disk.
        match open_append_writer(&self.path) {
            Ok(writer) => {
                self.writer = Some(writer);
                self.format_version = StorageVersion::V2Current;
                self.approx_log_bytes = compacted_size;
                Ok(())
            }
            Err(e) => {
                self.state = StorageState::Failed;
                // open_append_writer returns MezError (it goes through
                // the `?` / From<io::Error> conversion internally), but
                // this variant carries the raw io::Error — unwrap back
                // out to it, falling back to a generic io::Error for the
                // (practically unreachable here) non-Io MezError cases
                // rather than losing the failure entirely.
                let io_err = match e {
                    MezError::Io(io_err) => io_err,
                    other => io::Error::other(other.to_string()),
                };
                Err(MezError::CompactionInstalledButIncomplete(io_err))
            }
        }
    }

    fn append_record(&mut self, key: &[u8], value: &[u8], expires_at: Option<u64>, deleted: bool) -> Result<()> {
        if self.state == StorageState::Failed {
            return Err(MezError::FailedStorage);
        }
        self.check_limits_for_version(key.len(), value.len(), VERSION_2)?;

        self.record_buf.clear();
        encode_record_v2(&mut self.record_buf, key, value, expires_at, deleted)?;

        if let Some(max_log) = self.limits.max_log_bytes {
            let projected = self.approx_log_bytes.saturating_add(self.record_buf.len() as u64);
            if projected > max_log {
                return Err(MezError::NeedsCompaction);
            }
        }

        let write_result = (|| -> io::Result<()> {
            #[cfg(test)]
            if INJECT_APPEND_ERROR.with(|c| c.replace(false)) {
                return Err(injected_test_error());
            }
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| io::Error::other("writer unavailable"))?;
            writer.write_all(&self.record_buf)
        })();

        if let Err(e) = write_result {
            // Unknown partial-write state on disk: stop trusting this
            // writer for further mutation until the store is reopened.
            self.state = StorageState::Failed;
            return Err(MezError::Io(e));
        }
        self.approx_log_bytes = self.approx_log_bytes.saturating_add(self.record_buf.len() as u64);
        Ok(())
    }

    fn writer_mut(&mut self) -> Result<&mut BufWriter<File>> {
        self.writer.as_mut().ok_or(MezError::Closed)
    }
}

impl Drop for Mezaleth {
    /// Best-effort flush of any buffered-but-unwritten bytes when the
    /// instance is dropped without an explicit `close()`.
    ///
    /// `BufWriter` already attempts a flush in its own `Drop`, but
    /// silently discards the result — there is no way for the caller to
    /// learn a final flush failed. This doesn't change that fundamental
    /// limitation (`Drop::drop` still can't return a `Result`, so a
    /// failure here is likewise unobservable to the caller), but it does
    /// make the attempt explicit and self-documenting rather than
    /// relying on `BufWriter`'s implicit behavior, and it runs before
    /// `BufWriter`'s own drop flush, which then becomes a cheap no-op on
    /// an already-empty buffer. Errors are deliberately ignored — by the
    /// time `Drop::drop` runs there is no `Result` to report them
    /// through and no state left to mark `Failed` for a future caller.
    /// Callers who need to observe flush failures must call
    /// [`Self::close`], [`Self::flush`], or [`Self::sync`] explicitly
    /// before dropping.
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            let _ = writer.flush();
        }
    }
}

fn pre_install_err(e: MezError) -> MezError {
    match e {
        MezError::Io(io_err) => MezError::CompactionFailedBeforeInstall(io_err),
        other => other,
    }
}

fn open_append_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new().read(true).append(true).open(path)?;
    Ok(BufWriter::with_capacity(BUFFER_SIZE, file))
}

/// Best-effort cleanup of compaction temp files left behind by a process
/// that crashed mid-compaction. Only removes files whose name starts
/// with this exact database's temp-naming prefix
/// (`{filename}.tmp-{pid}-{nonce}-{attempt}`) in the same directory —
/// never anything else. Failures are swallowed: this is hygiene, not
/// correctness-critical, and the caller already holds the exclusive lock
/// so no live compaction's temp file can be mistaken for an orphan (a
/// live compacting instance would still hold the lock this function's
/// caller had to acquire first).
fn cleanup_orphaned_temp_files(path: &Path) {
    let Some(dir) = path.parent() else { return };
    let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
    let Some(stem) = path.file_name().and_then(|n| n.to_str()) else { return };
    let prefix = format!("{stem}.tmp-");
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
}

// --------------------------------------------------------------------
// Incremental CRC32 (IEEE 802.3)
// --------------------------------------------------------------------

fn crc32_table() -> &'static [u32; 256] {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        let mut i = 0u32;
        while i < 256 {
            let mut c = i;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
                k += 1;
            }
            table[i as usize] = c;
            i += 1;
        }
        table
    })
}

/// Incremental CRC-32 (IEEE) accumulator: `update()` may be called any
/// number of times with any chunking, equivalent to hashing the
/// concatenation of all chunks in one call.
struct Crc32 {
    crc: u32,
}
impl Crc32 {
    fn new() -> Self {
        Self { crc: 0xFFFF_FFFF }
    }
    #[inline]
    fn update(&mut self, data: &[u8]) {
        let table = crc32_table();
        let mut crc = self.crc;
        for &byte in data {
            let idx = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = table[idx] ^ (crc >> 8);
        }
        self.crc = crc;
    }
    #[inline]
    fn finalize(self) -> u32 {
        self.crc ^ 0xFFFF_FFFF
    }
}

// --------------------------------------------------------------------
// Record encoding / decoding
// --------------------------------------------------------------------

fn encode_record_v2(
    out: &mut Vec<u8>,
    key: &[u8],
    value: &[u8],
    expires_at: Option<u64>,
    deleted: bool,
) -> Result<()> {
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;
    let expires_raw = expires_at.unwrap_or(0);
    let flags = if deleted { FLAG_DELETED } else { 0 };

    let needed: usize = (FIXED_FIELDS_LEN_V2 as usize)
        .checked_add(key.len())
        .and_then(|v| v.checked_add(value.len()))
        .ok_or_else(|| {
            MezError::RecordTooLarge(SizeLimitViolation {
                kind: LimitKind::RecordLen,
                actual: u64::MAX,
                limit: u64::MAX,
                offset: None,
            })
        })?;
    out.reserve(needed);

    let mut crc = Crc32::new();
    let key_len_bytes = key_len.to_le_bytes();
    out.extend_from_slice(&key_len_bytes);
    crc.update(&key_len_bytes);
    out.extend_from_slice(key);
    crc.update(key);
    let value_len_bytes = value_len.to_le_bytes();
    out.extend_from_slice(&value_len_bytes);
    crc.update(&value_len_bytes);
    out.extend_from_slice(value);
    crc.update(value);
    let expires_bytes = expires_raw.to_le_bytes();
    out.extend_from_slice(&expires_bytes);
    crc.update(&expires_bytes);
    out.extend_from_slice(&[flags]);
    crc.update(&[flags]);
    out.extend_from_slice(&crc.finalize().to_le_bytes());
    Ok(())
}

enum RecordRead {
    Ok { consumed: u64, key: Vec<u8>, value: Vec<u8>, expires_raw: u64, deleted: bool },
    /// Stream ended before the record was structurally complete, or a
    /// length claimed more bytes than remain in the file (the two are
    /// indistinguishable from the length alone — see module docs).
    Incomplete,
    /// Structurally complete but checksum mismatch (v2 only).
    Corrupted,
    /// A length fit within the file's remaining bytes but exceeded
    /// configured [`Limits`]. Carries a violation with `offset: None` —
    /// the caller (which knows the record's start offset) fills it in.
    TooLarge(SizeLimitViolation),
}

/// Read and validate exactly one record, streaming field-by-field with
/// an incremental CRC (v2). For each length field, the remaining-file-
/// bytes budget is checked before the configured [`Limits`] check, so a
/// length which can't possibly fit in the file is always treated as an
/// incomplete tail rather than a size-limit violation. All arithmetic is
/// checked; malformed input can never trigger wraparound or an unbounded
/// allocation. The tombstone invariant (deleted => value_len == 0 &&
/// expires_at == 0) is enforced strictly.
fn read_record<R: Read>(
    reader: &mut R,
    version: u8,
    remaining_in_file: u64,
    limits: &Limits,
) -> Result<RecordRead> {
    let fixed_len: u64 = if version == VERSION_1 { FIXED_FIELDS_LEN_V1 } else { FIXED_FIELDS_LEN_V2 };
    let mut crc = Crc32::new();

    let mut key_len_buf = [0u8; 4];
    if read_exact_or_none(reader, &mut key_len_buf)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    let key_len = u32::from_le_bytes(key_len_buf);
    if version == VERSION_2 {
        crc.update(&key_len_buf);
    }
    let key_len_u64 = key_len as u64;

    // Bytes still required *after* the key bytes: value_len field(4) +
    // expires(8) + flags(1) [+ crc(4) for v2] — i.e. every fixed field
    // except the key_len field we've already consumed above. Using
    // `fixed_len - 8` here (subtracting both key_len AND value_len's
    // field widths) would double-count the key_len field that was
    // already subtracted from `remaining_in_file` via `.checked_sub(4)`
    // below, making this budget 4 bytes too generous.
    let min_tail_after_key: u64 = match fixed_len.checked_sub(4) {
        Some(v) => v,
        None => return Err(MezError::InvalidFormat),
    };
    let budget_for_key =
        remaining_in_file.checked_sub(4).and_then(|r| r.checked_sub(min_tail_after_key));
    match budget_for_key {
        None => return Ok(RecordRead::Incomplete),
        Some(budget) if key_len_u64 > budget => return Ok(RecordRead::Incomplete),
        _ => {}
    }
    if key_len as usize > limits.max_key_len as usize {
        return Ok(RecordRead::TooLarge(SizeLimitViolation {
            kind: LimitKind::KeyLen,
            actual: key_len_u64,
            limit: limits.max_key_len as u64,
            offset: None,
        }));
    }

    let mut key = vec![0u8; key_len as usize];
    if read_exact_or_none(reader, &mut key)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    if version == VERSION_2 {
        crc.update(&key);
    }

    let mut value_len_buf = [0u8; 4];
    if read_exact_or_none(reader, &mut value_len_buf)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    let value_len = u32::from_le_bytes(value_len_buf);
    if version == VERSION_2 {
        crc.update(&value_len_buf);
    }
    let value_len_u64 = value_len as u64;

    let consumed_so_far = 4u64
        .checked_add(key_len_u64)
        .and_then(|v| v.checked_add(4))
        .ok_or(MezError::InvalidFormat)?;
    let tail_fixed: u64 = match fixed_len.checked_sub(8) {
        Some(v) => v,
        None => return Err(MezError::InvalidFormat),
    };
    let budget_for_value =
        remaining_in_file.checked_sub(consumed_so_far).and_then(|r| r.checked_sub(tail_fixed));
    match budget_for_value {
        None => return Ok(RecordRead::Incomplete),
        Some(budget) if value_len_u64 > budget => return Ok(RecordRead::Incomplete),
        _ => {}
    }
    if value_len as usize > limits.max_value_len as usize {
        return Ok(RecordRead::TooLarge(SizeLimitViolation {
            kind: LimitKind::ValueLen,
            actual: value_len_u64,
            limit: limits.max_value_len as u64,
            offset: None,
        }));
    }

    let total_len: Option<u64> =
        fixed_len.checked_add(key_len_u64).and_then(|v| v.checked_add(value_len_u64));
    match total_len {
        None => {
            return Ok(RecordRead::TooLarge(SizeLimitViolation {
                kind: LimitKind::RecordLen,
                actual: u64::MAX,
                limit: limits.max_record_len,
                offset: None,
            }))
        }
        Some(total) if total > limits.max_record_len => {
            return Ok(RecordRead::TooLarge(SizeLimitViolation {
                kind: LimitKind::RecordLen,
                actual: total,
                limit: limits.max_record_len,
                offset: None,
            }))
        }
        _ => {}
    }

    let mut value = vec![0u8; value_len as usize];
    if read_exact_or_none(reader, &mut value)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    if version == VERSION_2 {
        crc.update(&value);
    }

    let mut expires_buf = [0u8; 8];
    if read_exact_or_none(reader, &mut expires_buf)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    let expires_raw = u64::from_le_bytes(expires_buf);
    if version == VERSION_2 {
        crc.update(&expires_buf);
    }

    let mut flags_buf = [0u8; 1];
    if read_exact_or_none(reader, &mut flags_buf)?.is_none() {
        return Ok(RecordRead::Incomplete);
    }
    let flags = flags_buf[0];
    if flags & !KNOWN_FLAGS != 0 {
        return Err(MezError::InvalidFormat);
    }
    if version == VERSION_2 {
        crc.update(&flags_buf);
    }
    let deleted = flags & FLAG_DELETED != 0;

    // Strict tombstone invariant: the encoder never produces a deleted
    // record with a nonzero value length or expiry. Seeing one means
    // corruption or a hostile file, not something to silently normalize.
    if deleted && (value_len_u64 != 0 || expires_raw != 0) {
        return Err(MezError::InvalidFormat);
    }

    let mut consumed: u64 = 4;
    consumed = consumed
        .checked_add(key_len_u64)
        .and_then(|v| v.checked_add(4))
        .and_then(|v| v.checked_add(value_len_u64))
        .and_then(|v| v.checked_add(8))
        .and_then(|v| v.checked_add(1))
        .ok_or(MezError::InvalidFormat)?;

    if version == VERSION_2 {
        let mut crc_buf = [0u8; 4];
        if read_exact_or_none(reader, &mut crc_buf)?.is_none() {
            return Ok(RecordRead::Incomplete);
        }
        let stored_crc = u32::from_le_bytes(crc_buf);
        consumed = consumed.checked_add(4).ok_or(MezError::InvalidFormat)?;
        if crc.finalize() != stored_crc {
            return Ok(RecordRead::Corrupted);
        }
    }

    Ok(RecordRead::Ok { consumed, key, value, expires_raw, deleted })
}

fn read_exact_or_none<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<Option<()>> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => return Ok(None),
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(MezError::Io(e)),
        }
    }
    Ok(Some(()))
}

// --------------------------------------------------------------------
// Exclusive temp-file creation
// --------------------------------------------------------------------

fn create_unique_temp_file(dest: &Path) -> Result<(PathBuf, File)> {
    let mut last_err = None;
    for attempt in 0..TEMP_FILE_CREATE_ATTEMPTS {
        let candidate = temp_path(dest, attempt);
        match OpenOptions::new().write(true).create_new(true).open(&candidate) {
            Ok(file) => return Ok((candidate, file)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(MezError::Io(e)),
        }
    }
    Err(MezError::Io(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::AlreadyExists, "could not create unique temp file")
    })))
}

#[cfg(test)]
thread_local! {
    static TEST_NONCE_OVERRIDE: std::cell::Cell<Option<u128>> = const { std::cell::Cell::new(None) };
    /// One-shot fault-injection flags, checked immediately before the
    /// real I/O call they name. Thread-local (not process-global) so
    /// parallel `cargo test` threads never interfere with each other's
    /// injected faults. Each flag auto-clears itself the moment it
    /// fires, so a test doesn't have to remember to reset it.
    static INJECT_APPEND_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INJECT_FLUSH_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INJECT_SYNC_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn injected_test_error() -> io::Error {
    io::Error::other("injected fault for testing")
}

/// Test-only fault injection: arms a one-shot flag that makes the named
/// operation fail with a synthetic I/O error on its *next* call on this
/// thread, exercising the same `Failed`-state transition a real disk
/// failure would trigger — without needing an actual faulty filesystem.
#[cfg(test)]
mod fault_injection {
    use super::*;

    pub fn inject_append_error() {
        INJECT_APPEND_ERROR.with(|c| c.set(true));
    }
    pub fn inject_flush_error() {
        INJECT_FLUSH_ERROR.with(|c| c.set(true));
    }
    pub fn inject_sync_error() {
        INJECT_SYNC_ERROR.with(|c| c.set(true));
    }
}

fn gen_nonce() -> u128 {
    #[cfg(test)]
    {
        if let Some(n) = TEST_NONCE_OVERRIDE.with(|c| c.get()) {
            return n;
        }
    }
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

fn temp_path(path: &Path, attempt: u32) -> PathBuf {
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("mezaleth");
    let nonce = gen_nonce();
    path.with_file_name(format!("{file_name}.tmp-{}-{nonce}-{attempt}", std::process::id()))
}

// --------------------------------------------------------------------
// Atomic-ish file replacement
// --------------------------------------------------------------------

fn replace_file_durably(src: &Path, dst: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::rename(src, dst)?;
        fsync_parent_dir(dst)?;
        Ok(())
    }
    #[cfg(windows)]
    {
        windows_replace_file(src, dst)
    }
    #[cfg(not(any(unix, windows)))]
    {
        // Best-effort, non-atomic fallback on platforms without a
        // supported replacement primitive — documented as weaker, never
        // claimed to be atomic. See module "Platform support" docs.
        if dst.exists() {
            fs::remove_file(dst)?;
        }
        fs::rename(src, dst)
    }
}

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
fn windows_replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn MoveFileExW(lpExistingFileName: *const u16, lpNewFileName: *const u16, dwFlags: u32) -> i32;
    }
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    fn to_wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(std::iter::once(0)).collect()
    }
    let src_wide = to_wide(src);
    let dst_wide = to_wide(dst);
    // SAFETY: both buffers are NUL-terminated and outlive this call.
    let ok = unsafe {
        MoveFileExW(src_wide.as_ptr(), dst_wide.as_ptr(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
    };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// --------------------------------------------------------------------
// Fuzzing support (behind the `fuzzing` feature; see /fuzz in the repo)
// --------------------------------------------------------------------

/// Entry points for `cargo fuzz`, gated behind the `fuzzing` Cargo
/// feature so they never appear in the normal public API or its docs.
/// These are declared here (rather than only inside the external
/// `fuzz/` crate) because the functions they call — `read_record`,
/// `VERSION_1`/`VERSION_2`, `MAGIC` — are private to this crate; a
/// separate fuzz crate can only reach them through a `pub` surface like
/// this one.
///
/// Two kinds of target are provided:
/// - `read_record_v1`/`read_record_v2` fuzz the record parser directly,
///   in memory, with no filesystem I/O — this is the fast, high-value
///   target, since nearly every interesting bug in this codebase (the
///   off-by-4 budget error fixed earlier; any future checked-arithmetic
///   or tombstone-invariant regression) lives inside `read_record`.
/// - `full_recovery` drives the same input through the real
///   `Mezaleth::open()` path (header validation, the streaming parse
///   loop, tail truncation, lock acquisition) via an actual temp file.
///   Slower, but catches bugs in the *loop* around `read_record` — e.g.
///   offset bookkeeping across multiple records — that a single-record
///   target can't see.
///
/// None of these ever call `.unwrap()` on the parse result itself: an
/// `Err` for malformed input is expected and correct. The only failure
/// mode a fuzz target is checking for is a panic or (for the
/// filesystem-backed target) a leaked temp/lock file.
#[cfg(feature = "fuzzing")]
pub mod fuzz_support {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Fuzz the v1 (no-CRC) record parser directly. `data` is treated as
    /// the bytes immediately following a v1 record's start; `read_record`
    /// is given `data.len()` as the "remaining bytes in file" bound, so
    /// every length/budget check it performs is exercised exactly as it
    /// would be during real recovery.
    pub fn read_record_v1(data: &[u8]) {
        let limits = Limits::default();
        let mut reader = data;
        let _ = read_record(&mut reader, VERSION_1, data.len() as u64, &limits);
    }

    /// Same as [`read_record_v1`] but for the v2 (CRC-checked, tombstone-
    /// validated) format — the more complex and more important target,
    /// since it exercises the incremental CRC accumulator and the
    /// strict tombstone invariant in addition to everything v1 does.
    pub fn read_record_v2(data: &[u8]) {
        let limits = Limits::default();
        let mut reader = data;
        let _ = read_record(&mut reader, VERSION_2, data.len() as u64, &limits);
    }

    /// Higher-level, filesystem-backed fuzz target: writes
    /// `MAGIC + version_byte + data` to a real temporary file and runs
    /// it through the full `Mezaleth::open()` recovery path. `data`'s
    /// first byte selects v1 vs v2 (via its low bit) so the fuzzer can
    /// explore both formats from one corpus; the rest of `data` becomes
    /// the log body. Cleans up its own temp and lock files regardless of
    /// outcome — a fuzz run that leaks files on every malformed input
    /// would fill the disk long before finding an interesting bug.
    pub fn full_recovery(data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let version = if data[0] & 1 == 0 { VERSION_1 } else { VERSION_2 };
        let body = &data[1..];

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("mezaleth_fuzz_{}_{id}.bin", std::process::id()));

        let mut bytes = Vec::with_capacity(MAGIC.len() + 1 + body.len());
        bytes.extend_from_slice(MAGIC);
        bytes.push(version);
        bytes.extend_from_slice(body);

        if std::fs::write(&path, &bytes).is_err() {
            return;
        }

        // Deliberately not unwrapped: Err is the expected, correct
        // outcome for most fuzzer-generated input. Only a panic here is
        // a bug.
        let _ = Mezaleth::open(&path);

        let _ = std::fs::remove_file(&path);
        let mut lock_path = path.into_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
    }
}

// --------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        path.push(format!("mezaleth_test_{name}_{}_{}.bin", std::process::id(), nonce));
        cleanup(&path);
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let mut lock = path.as_os_str().to_owned();
        lock.push(".lock");
        let _ = fs::remove_file(PathBuf::from(lock));
    }

    #[test]
    fn basic_set_get_remove() {
        let path = tmp_path("basic");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("name", "ali").unwrap();
        store.set("lang", "rust").unwrap();
        assert_eq!(store.get("name"), Some(&b"ali"[..]));
        store.remove("name").unwrap();
        assert_eq!(store.get("name"), None);
        store.flush().unwrap();
        cleanup(&path);
    }

    #[test]
    fn persist_and_reopen() {
        let path = tmp_path("persist");
        {
            let mut store = Mezaleth::open_or_create(&path).unwrap();
            store.set("a", "1").unwrap();
            store.set("b", "2").unwrap();
            store.remove("a").unwrap();
            store.close().unwrap();
        }
        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("b"), Some(&b"2"[..]));
        cleanup(&path);
    }

    #[test]
    fn open_or_create_does_not_truncate_existing() {
        let path = tmp_path("no_truncate");
        {
            let mut store = Mezaleth::open_or_create(&path).unwrap();
            store.set("k", "v").unwrap();
            store.close().unwrap();
        }
        let store = Mezaleth::open_or_create(&path).unwrap();
        assert_eq!(store.get("k"), Some(&b"v"[..]));
        cleanup(&path);
    }

    #[test]
    fn create_new_fails_if_exists() {
        let path = tmp_path("create_new_exists");
        {
            let mut store = Mezaleth::create_new(&path).unwrap();
            store.close().unwrap();
        }
        let result = Mezaleth::create_new(&path);
        assert!(matches!(result, Err(MezError::Io(_))));
        cleanup(&path);
    }

    #[test]
    fn second_open_in_same_process_is_rejected() {
        let path = tmp_path("double_open");
        let _first = Mezaleth::open_or_create(&path).unwrap();
        let second = Mezaleth::open(&path);
        assert!(matches!(second, Err(MezError::AlreadyOpen)));
        cleanup(&path);
    }

    #[test]
    fn lock_released_after_close_allows_reopen() {
        let path = tmp_path("lock_release");
        let mut first = Mezaleth::open_or_create(&path).unwrap();
        first.close().unwrap();
        drop(first);
        let second = Mezaleth::open(&path);
        assert!(second.is_ok());
        cleanup(&path);
    }

    #[test]
    fn failed_state_blocks_further_writes() {
        let path = tmp_path("failed_state");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();
        // Directly force Failed, mirroring what append_record does on a
        // real write error (see module docs) without needing a real
        // fault-injection harness.
        store.state = StorageState::Failed;

        assert!(matches!(store.set("b", "2"), Err(MezError::FailedStorage)));
        assert!(matches!(store.set_with_ttl("b", "2", 5), Err(MezError::FailedStorage)));
        assert!(matches!(store.remove("a"), Err(MezError::FailedStorage)));
        assert!(matches!(store.compact(), Err(MezError::FailedStorage)));
        cleanup(&path);
    }

    #[test]
    fn v1_file_is_genuinely_read_only_handle() {
        let path = tmp_path("v1_readonly_handle");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION_1);
        let key = b"legacy";
        let value = b"data";
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(0);
        fs::write(&path, &bytes).unwrap();

        let mut store = Mezaleth::open(&path).unwrap();
        assert!(store.is_legacy_read_only());
        assert!(store.writer.is_none());
        assert!(matches!(store.set("a", "1"), Err(MezError::LegacyReadOnly)));

        store.compact().unwrap();
        assert!(!store.is_legacy_read_only());
        assert!(store.writer.is_some());
        store.set("b", "2").unwrap();
        store.close().unwrap();
        drop(store); // release the lock before reopening (see note above)

        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("legacy"), Some(&b"data"[..]));
        assert_eq!(store.get("b"), Some(&b"2"[..]));
        cleanup(&path);
    }

    #[test]
    fn v1_migration_respects_v2_limits_with_crc_overhead() {
        let path = tmp_path("v1_migration_limit");
        let key = b"k";
        let value = vec![0u8; 10];
        let v1_total = FIXED_FIELDS_LEN_V1 + key.len() as u64 + value.len() as u64;
        let limits = Limits::new(1024, 1024).with_max_record_len(v1_total);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION_1);
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&value);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(0);
        fs::write(&path, &bytes).unwrap();

        let mut store = Mezaleth::open_with_limits(&path, limits).unwrap();
        let result = store.compact();
        assert!(matches!(result, Err(MezError::RecordTooLarge(_))));

        let on_disk = fs::read(&path).unwrap();
        assert_eq!(on_disk[MAGIC.len()], VERSION_1);
        cleanup(&path);
    }

    #[test]
    fn compacting_v1_file_upgrades_to_v2() {
        let path = tmp_path("v1_upgrade");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION_1);
        let key = b"k";
        let value = b"v";
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(0);
        fs::write(&path, &bytes).unwrap();

        let mut store = Mezaleth::open(&path).unwrap();
        store.compact().unwrap();
        store.close().unwrap();
        drop(store); // release the lock before reopening (see note above)

        let on_disk = fs::read(&path).unwrap();
        assert_eq!(on_disk[MAGIC.len()], VERSION_2);

        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("k"), Some(&b"v"[..]));
        cleanup(&path);
    }

    #[cfg(unix)]
    #[test]
    fn compaction_preserves_unix_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let path = tmp_path("perms");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();
        store.flush().unwrap();

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        store.compact().unwrap();

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        cleanup(&path);
    }

    #[test]
    fn overwrite_and_compact() {
        let path = tmp_path("compact");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("x", "1").unwrap();
        store.set("x", "2").unwrap();
        store.set("y", "3").unwrap();
        store.remove("y").unwrap();
        store.compact().unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("x"), Some(&b"2"[..]));
        store.close().unwrap();
        // `store` must be dropped (releasing the file lock held by its
        // `_lock` field) before reopening — a `let` shadow does not drop
        // the previous binding early, it stays alive until end of scope.
        drop(store);
        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("x"), Some(&b"2"[..]));
        assert_eq!(store.get("y"), None);
        cleanup(&path);
    }

    #[test]
    fn key_value_size_limits_enforced() {
        let path = tmp_path("limits");
        let mut store = Mezaleth::create_new_with_limits(&path, Limits::new(4, 4)).unwrap();
        store.set("abcd", "wxyz").unwrap();
        assert!(matches!(store.set("toolong", "v"), Err(MezError::RecordTooLarge(_))));
        assert!(matches!(store.set("k", "toolong"), Err(MezError::RecordTooLarge(_))));
        cleanup(&path);
    }

    #[test]
    fn record_too_large_carries_structured_diagnostics() {
        let path = tmp_path("structured_limit_err");
        let mut store = Mezaleth::create_new_with_limits(&path, Limits::new(4, 1024)).unwrap();

        match store.set("toolong-key", "v") {
            Err(MezError::RecordTooLarge(v)) => {
                assert_eq!(v.kind, LimitKind::KeyLen);
                assert_eq!(v.actual, 11);
                assert_eq!(v.limit, 4);
                // Not yet on disk, so no offset for a write-time violation.
                assert_eq!(v.offset, None);
            }
            other => panic!("expected RecordTooLarge, got {other:?}"),
        }
        cleanup(&path);

        let path2 = tmp_path("structured_limit_err2");
        let mut store2 = Mezaleth::create_new_with_limits(&path2, Limits::new(1024, 4)).unwrap();
        match store2.set("k", "toolongvalue") {
            Err(MezError::RecordTooLarge(v)) => {
                assert_eq!(v.kind, LimitKind::ValueLen);
                assert_eq!(v.actual, 12);
                assert_eq!(v.limit, 4);
            }
            other => panic!("expected RecordTooLarge, got {other:?}"),
        }
        cleanup(&path2);
    }

    #[test]
    fn ttl_zero_is_deletion_writes_no_value_record() {
        let path = tmp_path("ttl_zero_delete");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("k", "v").unwrap();
        store.set_with_ttl("k", "ignored", 0).unwrap();
        assert_eq!(store.get("k"), None);
        cleanup(&path);
    }

    #[test]
    fn max_entries_rejects_new_key_over_limit_but_allows_overwrite() {
        let path = tmp_path("max_entries");
        let limits = Limits::new(1024, 1024).with_max_entries(2);
        let mut store = Mezaleth::create_new_with_limits(&path, limits).unwrap();
        store.set("a", "1").unwrap();
        store.set("b", "2").unwrap();
        store.set("a", "11").unwrap(); // overwrite must still be allowed at cap
        assert!(matches!(store.set("c", "3"), Err(MezError::NeedsCompaction)));
        assert_eq!(store.len(), 2);
        cleanup(&path);
    }

    #[test]
    fn max_index_bytes_rejects_over_budget() {
        let path = tmp_path("max_index_bytes");
        // Small enough that a single reasonably sized entry blows the
        // budget once APPROX_ENTRY_OVERHEAD is included.
        let limits = Limits::new(1024, 1024).with_max_index_bytes(20);
        let mut store = Mezaleth::create_new_with_limits(&path, limits).unwrap();
        let result = store.set("key", "value");
        assert!(matches!(result, Err(MezError::NeedsCompaction)));
        assert_eq!(store.len(), 0);
        cleanup(&path);
    }

    #[test]
    fn max_index_bytes_running_total_tracks_overwrite_remove_and_purge() {
        // Regression test for the incremental `approx_index_bytes`
        // running total (replacing an O(n) full re-sum on every write):
        // proves it stays correct — not just initially, but after
        // overwrites, removes, and TTL-based purges — by observing that
        // the NeedsCompaction/success boundary tracks reality rather
        // than drifting.
        let path = tmp_path("max_index_bytes_running_total");
        // Each entry here costs 1 (key) + 10 (value) + 48 (overhead) = 59.
        let limits = Limits::new(1024, 1024).with_max_index_bytes(180); // room for 3, not 4
        let mut store = Mezaleth::create_new_with_limits(&path, limits).unwrap();

        store.set("a", "1111111111").unwrap();
        store.set("b", "2222222222").unwrap();
        store.set("c", "3333333333").unwrap();

        // A fourth same-sized entry must not fit (3*59=177, +59=236>180).
        assert!(matches!(store.set("d", "4444444444"), Err(MezError::NeedsCompaction)));

        // Overwriting an existing key with a same-sized value must not
        // change the total (old bytes freed == new bytes charged).
        store.set("a", "9999999999").unwrap();
        assert!(matches!(store.set("d", "4444444444"), Err(MezError::NeedsCompaction)));

        // Removing an entry must free its bytes so a new one now fits.
        store.remove("a").unwrap();
        store.set("d", "4444444444").unwrap();
        assert_eq!(store.len(), 3);

        cleanup(&path);
    }

    #[test]
    fn max_index_bytes_running_total_tracks_ttl_purge() {
        let path = tmp_path("max_index_bytes_purge");
        let limits = Limits::new(1024, 1024).with_max_index_bytes(70); // room for ~1 entry
        let mut store = Mezaleth::create_new_with_limits(&path, limits).unwrap();

        // Expires essentially immediately (1s TTL, checked against
        // whole-second wall clock so this may already be expired by the
        // time purge_expired() runs, which is fine for this test).
        store.set_with_ttl("a", "1111111111", 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));

        assert!(matches!(store.set("b", "2222222222"), Err(MezError::NeedsCompaction)));

        store.purge_expired();
        store.set("b", "2222222222").unwrap();
        cleanup(&path);
    }

    #[test]
    fn max_log_bytes_rejects_before_writing() {
        let path = tmp_path("max_log_bytes");
        let limits = Limits::new(1024, 1024).with_max_log_bytes(64);
        let mut store = Mezaleth::create_new_with_limits(&path, limits).unwrap();
        let before = fs::metadata(&path).unwrap().len();
        let result = store.set("this-key-is-long-enough", "to overflow the sixty four byte budget");
        assert!(matches!(result, Err(MezError::NeedsCompaction)));
        let after = fs::metadata(&path).unwrap().len();
        assert_eq!(before, after);
        cleanup(&path);
    }

    #[test]
    fn tombstone_with_nonzero_value_len_is_invalid_format() {
        let path = tmp_path("bad_tombstone");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION_2);
        let key = b"k";
        let mut body = Vec::new();
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(&3u32.to_le_bytes());
        body.extend_from_slice(b"xyz");
        body.extend_from_slice(&0u64.to_le_bytes());
        body.push(FLAG_DELETED);
        let mut crc = Crc32::new();
        crc.update(&body);
        body.extend_from_slice(&crc.finalize().to_le_bytes());
        bytes.extend_from_slice(&body);
        fs::write(&path, bytes).unwrap();

        assert!(matches!(Mezaleth::open(&path), Err(MezError::InvalidFormat)));
        cleanup(&path);
    }

    #[test]
    fn tombstone_with_nonzero_expiry_is_invalid_format() {
        let path = tmp_path("bad_tombstone_expiry");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION_2);
        let key = b"k";
        let mut body = Vec::new();
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&12345u64.to_le_bytes()); // nonzero expiry
        body.push(FLAG_DELETED);
        let mut crc = Crc32::new();
        crc.update(&body);
        body.extend_from_slice(&crc.finalize().to_le_bytes());
        bytes.extend_from_slice(&body);
        fs::write(&path, bytes).unwrap();

        assert!(matches!(Mezaleth::open(&path), Err(MezError::InvalidFormat)));
        cleanup(&path);
    }

    #[test]
    fn key_len_budget_is_exact_not_off_by_four() {
        // Regression test for a fixed off-by-4 in read_record()'s
        // key_len budget: the bytes still required *after* the key
        // bytes are `fixed_len - 4` (every fixed field except the
        // key_len field already consumed) — not `fixed_len - 8`, which
        // was 4 bytes too generous and could let an oversized key_len
        // pass the early budget check before eventually still being
        // rejected (as Incomplete) once the short read was attempted.
        // Calls read_record() directly (white-box) since the old bug's
        // only externally observable effect was a wasted, bounded
        // allocation in a narrow window — the final Result variant was
        // unchanged either way, so this couldn't be caught through the
        // public API alone.
        let limits = Limits::default();
        let key = b"ab";
        let mut data = Vec::new();
        data.extend_from_slice(&(key.len() as u32).to_le_bytes()); // key_len = 2
        data.extend_from_slice(key);
        data.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
        data.extend_from_slice(&0u64.to_le_bytes()); // expires_at = 0
        data.push(0); // flags

        // Exactly enough bytes for the whole v1 record: must parse.
        let remaining_exact = data.len() as u64;
        let mut reader: &[u8] = &data;
        let result = read_record(&mut reader, VERSION_1, remaining_exact, &limits).unwrap();
        assert!(matches!(result, RecordRead::Ok { .. }), "exact-fit record must parse successfully");

        // One byte short of what the record actually needs: the budget
        // check (or the subsequent short read) must report this as an
        // incomplete tail, never as a successful parse.
        let remaining_short = remaining_exact - 1;
        let mut reader: &[u8] = &data;
        let result = read_record(&mut reader, VERSION_1, remaining_short, &limits).unwrap();
        assert!(matches!(result, RecordRead::Incomplete), "one-byte-short record must be Incomplete");
    }

    #[test]
    fn orphaned_temp_file_is_cleaned_up_on_open() {
        let path = tmp_path("orphan_cleanup");
        {
            let mut store = Mezaleth::open_or_create(&path).unwrap();
            store.set("a", "1").unwrap();
            store.close().unwrap();
        }
        let orphan = temp_path(&path, 0);
        fs::write(&orphan, b"stale partial compaction data").unwrap();
        assert!(orphan.exists());

        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("a"), Some(&b"1"[..]));
        assert!(!orphan.exists(), "orphaned temp file should be removed on open");
        cleanup(&path);
    }

    #[test]
    fn temp_file_collision_is_retried_deterministically() {
        let path = tmp_path("temp_collision_det");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();

        TEST_NONCE_OVERRIDE.with(|c| c.set(Some(0xDEAD_BEEF)));
        let first_candidate = temp_path(&path, 0);
        OpenOptions::new().write(true).create_new(true).open(&first_candidate).unwrap();

        store.compact().unwrap();
        TEST_NONCE_OVERRIDE.with(|c| c.set(None));

        assert_eq!(store.get("a"), Some(&b"1"[..]));
        assert!(first_candidate.exists());
        fs::remove_file(&first_candidate).unwrap();
        cleanup(&path);
    }

    #[test]
    fn close_success_then_writes_return_closed() {
        let path = tmp_path("close_success");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();
        store.close().unwrap();
        assert!(matches!(store.set("b", "2"), Err(MezError::Closed)));
        drop(store);
        let reopened = Mezaleth::open(&path).unwrap();
        assert_eq!(reopened.get("a"), Some(&b"1"[..]));
        cleanup(&path);
    }

    #[test]
    fn crc32_incremental_matches_single_call() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let mut whole = Crc32::new();
        whole.update(data);
        let whole_result = whole.finalize();
        let mut chunked = Crc32::new();
        for chunk in data.chunks(3) {
            chunked.update(chunk);
        }
        assert_eq!(whole_result, chunked.finalize());
    }

    #[test]
    fn binary_keys_and_values() {
        let path = tmp_path("binary");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        let key: Vec<u8> = (0u8..=255).rev().collect();
        let value: Vec<u8> = (0u8..=255).collect();
        store.set(&key, &value).unwrap();
        assert_eq!(store.get(&key), Some(value.as_slice()));
        cleanup(&path);
    }

    #[test]
    fn empty_key_and_value() {
        let path = tmp_path("empty_kv");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set(b"" as &[u8], b"" as &[u8]).unwrap();
        store.close().unwrap();
        drop(store); // release the lock before reopening (see note above)
        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get(b"" as &[u8]), Some(&b""[..]));
        cleanup(&path);
    }

    #[test]
    fn large_streaming_recovery() {
        let path = tmp_path("large_recovery");
        {
            let mut store = Mezaleth::open_or_create(&path).unwrap();
            for i in 0..2000u32 {
                store.set(format!("key-{i}"), vec![b'x'; 200]).unwrap();
            }
            store.close().unwrap();
        }
        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.len(), 2000);
        assert_eq!(store.get("key-1999"), Some(vec![b'x'; 200].as_slice()));
        cleanup(&path);
    }

    // ----------------------------------------------------------------
    // Fault injection: deterministic write/flush/sync failure tests.
    // ----------------------------------------------------------------

    #[test]
    fn injected_write_error_transitions_to_failed_and_blocks_further_writes() {
        let path = tmp_path("fault_write");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();

        fault_injection::inject_append_error();
        let result = store.set("b", "2");
        assert!(matches!(result, Err(MezError::Io(_))));
        assert!(store.is_failed());

        // The store must not silently keep accepting writes after an
        // unknown-partial-write failure.
        assert!(matches!(store.set("c", "3"), Err(MezError::FailedStorage)));
        assert!(matches!(store.remove("a"), Err(MezError::FailedStorage)));
        assert!(matches!(store.compact(), Err(MezError::FailedStorage)));

        // The in-memory index must not have been updated for the failed
        // write (append happens before the index mutation in set_inner).
        assert_eq!(store.get("b"), None);

        // The fault was one-shot: a fresh instance is unaffected and can
        // recover the data that was durably written before the fault.
        drop(store);
        let store = Mezaleth::open(&path).unwrap();
        assert_eq!(store.get("a"), Some(&b"1"[..]));
        assert_eq!(store.get("b"), None);
        cleanup(&path);
    }

    #[test]
    fn injected_flush_error_transitions_to_failed() {
        let path = tmp_path("fault_flush");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();

        fault_injection::inject_flush_error();
        let result = store.flush();
        assert!(matches!(result, Err(MezError::Io(_))));
        assert!(store.is_failed());
        assert!(matches!(store.set("b", "2"), Err(MezError::FailedStorage)));
        cleanup(&path);
    }

    #[test]
    fn injected_sync_error_transitions_to_failed() {
        let path = tmp_path("fault_sync");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();

        fault_injection::inject_sync_error();
        let result = store.sync();
        assert!(matches!(result, Err(MezError::Io(_))));
        assert!(store.is_failed());
        assert!(matches!(store.set("b", "2"), Err(MezError::FailedStorage)));
        cleanup(&path);
    }

    #[test]
    fn injected_fault_is_one_shot_and_does_not_leak_to_next_operation() {
        // Arming the flag and then performing an *unrelated* operation
        // (a read) must not consume or be affected by it; only the next
        // matching operation (here, a write) should see the fault.
        let path = tmp_path("fault_one_shot");
        let mut store = Mezaleth::open_or_create(&path).unwrap();
        store.set("a", "1").unwrap();

        fault_injection::inject_append_error();
        assert_eq!(store.get("a"), Some(&b"1"[..])); // read: unaffected
        assert!(store.set("b", "2").is_err()); // write: consumes the fault
        assert!(store.is_failed());
        cleanup(&path);
    }

    // ----------------------------------------------------------------
    // Two-process locking. `lock_helper_hold_lock` is spawned as a
    // subprocess (via re-invoking this same test binary) by
    // `two_processes_cannot_open_same_file_simultaneously`; it is
    // `#[ignore]`d so a normal `cargo test` run never executes it
    // directly.
    // ----------------------------------------------------------------

    #[test]
    #[ignore]
    fn lock_helper_hold_lock() {
        let path = PathBuf::from(
            std::env::var("MEZALETH_LOCK_TEST_PATH")
                .expect("MEZALETH_LOCK_TEST_PATH must be set by the parent test process"),
        );
        let ready_path = path.with_extension("ready");
        let stop_path = path.with_extension("stop");

        let _store = Mezaleth::open_or_create(&path).expect("helper process failed to acquire lock");
        fs::write(&ready_path, b"ready").expect("helper process failed to write ready marker");

        // Bounded wait for the parent's stop signal, with a safety
        // timeout so a bug in the parent can't leak this process
        // forever in CI.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !stop_path.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // `_store` drops here, releasing the exclusive lock.
    }

    #[test]
    fn two_processes_cannot_open_same_file_simultaneously() {
        let path = tmp_path("two_process_lock");
        let ready_path = path.with_extension("ready");
        let stop_path = path.with_extension("stop");
        let _ = fs::remove_file(&ready_path);
        let _ = fs::remove_file(&stop_path);

        let exe = std::env::current_exe().expect("could not resolve current test binary path");
        let mut child = std::process::Command::new(&exe)
            .args(["tests::lock_helper_hold_lock", "--exact", "--ignored", "--test-threads=1"])
            .env("MEZALETH_LOCK_TEST_PATH", &path)
            .spawn()
            .expect("failed to spawn helper process");

        // Wait for the child to actually hold the lock — bounded polling
        // on its readiness marker, not a fixed sleep, so this isn't
        // flaky under CI scheduling jitter (beyond the timeout itself).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready_path.exists() {
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                cleanup(&path);
                let _ = fs::remove_file(&ready_path);
                let _ = fs::remove_file(&stop_path);
                panic!("helper process never became ready within 5s");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        // The child holds the exclusive lock; this process must be
        // refused rather than silently corrupting a concurrently-open
        // database.
        let result = Mezaleth::open(&path);
        assert!(matches!(result, Err(MezError::AlreadyOpen)));

        // Release the child and wait for it to actually exit, so its
        // lock handle is genuinely closed before we verify reopening.
        fs::write(&stop_path, b"stop").unwrap();
        let status = child.wait().expect("failed to wait for helper process");
        assert!(status.success(), "helper process exited with failure: {status:?}");

        // Now that the other process is gone, opening must succeed.
        let reopened = Mezaleth::open(&path);
        assert!(reopened.is_ok());

        let _ = fs::remove_file(&ready_path);
        let _ = fs::remove_file(&stop_path);
        cleanup(&path);
    }
}

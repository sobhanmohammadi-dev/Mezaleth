# Mezaleth API Reference

**Version:** 0.1.0
**Crate:** `Mezaleth`

Mezaleth is a small, single-process, append-only, in-memory-indexed
key-value store for Rust. This document is a complete reference to its
public API: every type, every method, what each one guarantees, what it
doesn't, and worked examples for every non-trivial usage pattern.

For the on-disk format and internal design rationale, see the
module-level doc comment at the top of `src/lib.rs` — this document
focuses on *how to use the library correctly*, not its internals.

---

## Table of Contents

1. [Design Summary](#1-design-summary)
2. [Getting Started](#2-getting-started)
3. [The `Mezaleth` Type](#3-the-mezaleth-type)
4. [Construction](#4-construction)
5. [Reading](#5-reading)
6. [Writing](#6-writing)
7. [Deletion & TTL](#7-deletion--ttl)
8. [Lifecycle & Durability](#8-lifecycle--durability)
9. [Compaction](#9-compaction)
10. [Resource Limits](#10-resource-limits)
11. [Error Handling](#11-error-handling)
12. [Known Limitations](#12-known-limitations)
13. [Concurrency & Locking Model](#13-concurrency--locking-model)
14. [Common Recipes](#14-common-recipes)
15. [Full API Index](#15-full-api-index)

---

## 1. Design Summary

| Property | Value |
|---|---|
| Storage model | Append-only log on disk + full in-memory `HashMap` index |
| Process model | Single owner per file, enforced by an OS-level exclusive lock |
| Durability | Explicit — you choose buffered / flushed / synced |
| Corruption detection | CRC-32 on every v2 record |
| Concurrency | None internally — wrap in `Mutex`/`RwLock` for multi-threaded use |
| Format versions | v1 (legacy, read-only) and v2 (current, CRC-checked) |

Mezaleth trades scale for simplicity and rigor: it is designed for
datasets whose live index comfortably fits in memory, and it makes every
effort to have honest, explicit failure semantics rather than silently
degrading. If you need a store for datasets that don't fit in RAM, or
true multi-writer concurrency, this is not the right tool.

---

## 2. Getting Started

```toml
# Cargo.toml
[dependencies]
Mezaleth = { path = "../Mezaleth" }  # or a registry version, once published
```

```rust
use Mezaleth::Mezaleth;

fn main() -> Mezaleth::Result<()> {
    // Opens the file if it exists, creates it if it doesn't.
    // Never truncates existing data.
    let mut store = Mezaleth::open_or_create("my_database.mez")?;

    store.set("username", "alice")?;
    store.set_with_ttl("session_token", "abc123", 3600)?; // expires in 1 hour

    if let Some(value) = store.get("username") {
        println!("username = {}", String::from_utf8_lossy(value));
    }

    store.close()?; // flush and release the lock cleanly
    Ok(())
}
```

That's the whole surface most applications need: `open_or_create`,
`set`, `get`, `close`. Everything below this point is about the parts
you reach for when you need more control.

---

## 3. The `Mezaleth` Type

```rust
pub struct Mezaleth { /* private fields */ }
```

`Mezaleth` is the single type this crate exposes as a database handle.
One instance owns:

- an exclusive OS-level lock on the database file (held for the
  instance's entire lifetime),
- the open file handle and its buffered writer (absent for a read-only
  legacy v1 file — see [§4.3](#43-opening-an-existing-database)),
- the full in-memory index (`key → value`, with optional expiration),
- the instance's [`Limits`](#10-resource-limits) policy,
- and a small internal state machine: `Open`, `Failed`, or `Closed`
  (see [§8](#8-lifecycle--durability)).

There is no `Clone`, `Copy`, or `Send`/`Sync` impl provided — a
`Mezaleth` instance is not safe to share across threads without external
synchronization, because the exclusive file lock and the in-memory index
are both tied to this one instance. Wrap it in `Arc<Mutex<Mezaleth>>` if
you need shared access from multiple threads.

---

## 4. Construction

Mezaleth deliberately splits "create" into explicit, differently-named
constructors rather than one function with ambiguous truncate/overwrite
behavior. Pick based on what you actually want to happen when the file
already exists.

| Function | If file exists | If file doesn't exist |
|---|---|---|
| `open_or_create` | Opens it (recovers data) | Creates a new v2 file |
| `open` | Opens it (recovers data) | Returns `Io` error (`NotFound`) |
| `create_new` | Returns `Io` error (`AlreadyExists`) | Creates a new v2 file |
| `create_truncate` | **Destroys existing content**, creates fresh | Creates a new v2 file |

Every constructor has a `_with_limits` sibling that takes an explicit
[`Limits`](#10-resource-limits) value instead of `Limits::default()`.

Every constructor also **acquires an exclusive lock** on the database
file (see [§13](#13-concurrency--locking-model)) before doing anything
else, and fails with `MezError::AlreadyOpen` if another live instance —
in this process or another — already holds it.

### 4.1. `open_or_create` — the one you usually want

```rust
pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self>
pub fn open_or_create_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self>
```

Atomic and non-destructive: internally this always attempts an
exclusive `create_new` first and only falls back to `open()` if that
loses a race to a concurrent creator. There is no window in which two
callers can both observe "doesn't exist" and one of them destroys the
other's freshly-created data.

```rust
use Mezaleth::Mezaleth;

let mut store = Mezaleth::open_or_create("app.mez")?;
// Safe to call this on every application startup — it will never wipe
// an existing database, whether this is the first run or the hundredth.
```

### 4.2. `create_new` / `create_truncate` — explicit intent

```rust
pub fn create_new<P: AsRef<Path>>(path: P) -> Result<Self>
pub fn create_truncate<P: AsRef<Path>>(path: P) -> Result<Self>
```

Use `create_new` when "the file must not already exist" is itself a
correctness requirement (e.g. provisioning a brand-new tenant's
database and treating a collision as a bug):

```rust
match Mezaleth::create_new(format!("tenants/{tenant_id}.mez")) {
    Ok(store) => { /* first-time setup */ }
    Err(Mezaleth::MezError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => {
        panic!("tenant database already provisioned — this should never happen");
    }
    Err(e) => return Err(e),
}
```

Use `create_truncate` only when you genuinely intend to discard
whatever is currently at that path — for example, resetting a test
fixture between test runs:

```rust
#[cfg(test)]
fn fresh_test_db() -> Mezaleth::Mezaleth {
    // Explicitly destructive — the name says so, so nobody calling this
    // in production code paths could mistake it for the safe default.
    Mezaleth::create_truncate("/tmp/test.mez").unwrap()
}
```

> **Why not just have one `create()` that truncates?** An earlier
> revision of this API did exactly that, and it's exactly the kind of
> footgun this document exists to help you avoid: a single ambiguously-
> named constructor that silently destroys data is a production
> incident waiting to happen. `create()` and `newstorage()` still exist
> as `#[deprecated]` aliases for `create_truncate()`, purely for
> backward compatibility — new code should not use them.

### 4.3. Opening an existing database

```rust
pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
pub fn open_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self>
```

`open()` requires the file to already exist and be a valid Mezaleth
database (correct magic bytes and a known format version), and runs full
recovery: it streams the log, replays every record into the in-memory
index, validates checksums (v2) and structural invariants, and — if the
file ends mid-record (e.g. the process crashed during a previous write)
— truncates the file back to the last complete, valid record before
returning.

```rust
match Mezaleth::open("app.mez") {
    Ok(store) => { /* ready to use */ }
    Err(Mezaleth::MezError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
        eprintln!("no database yet — run setup first");
    }
    Err(Mezaleth::MezError::CorruptedRecord { offset }) => {
        eprintln!("database is corrupted at byte {offset} — restore from backup");
    }
    Err(e) => return Err(e),
}
```

**Opening a legacy v1 file** gives you a **genuinely read-only**
instance — not merely one that rejects writes at the API layer, but one
whose underlying file handle was never opened for writing at all. Check
with `is_legacy_read_only()`:

```rust
let mut store = Mezaleth::open("old_format.mez")?;
if store.is_legacy_read_only() {
    println!("legacy v1 file — upgrading to v2");
    store.compact()?; // rewrites in the current format; store becomes writable
}
store.set("now this works", "yes")?;
```

---

## 5. Reading

```rust
pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]>
pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> bool
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
```

All four are pure in-memory operations — no I/O, no allocation for
`get`/`contains` (the returned slice borrows directly from the index).

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
store.set("counter", "42")?;

assert_eq!(store.get("counter"), Some(&b"42"[..]));
assert!(store.contains("counter"));
assert!(!store.contains("missing_key"));
assert_eq!(store.len(), 1);
assert!(!store.is_empty());
```

**A key subtlety with `len()`:** it counts every *indexed* key,
including ones whose TTL has expired but haven't been purged yet.
`get()` and `contains()` both treat an expired entry as absent — but it
still counts toward `len()` until you call `purge_expired()` or it's
overwritten. See [§7.3](#73-understanding-len-and-expired-entries).

`get()` never blocks, never allocates, and is safe to call at any point
in the instance's lifecycle — even after `close()` or a `Failed`
transition, since reads don't touch the (possibly gone) writer.

---

## 6. Writing

```rust
pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()>
pub fn set_with_ttl<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V, ttl_secs: u64) -> Result<()>
```

`set` writes a record to the on-disk log (buffered — see
[§8](#8-lifecycle--durability) for what "written" actually guarantees)
and then updates the in-memory index. **The write always happens
first**: if the disk write fails, the in-memory index is left
untouched, so a failed `set()` can never leave memory and disk
disagreeing about whether a write happened.

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;

// Both key and value accept anything implementing AsRef<[u8]> — &str,
// String, Vec<u8>, &[u8], etc. — so no manual conversion is needed for
// common cases.
store.set("string_key", "string_value")?;
store.set(b"byte_key".to_vec(), vec![1, 2, 3, 4])?;
store.set(format!("user:{}", 42), serde_json::to_vec(&user)?)?;

// Overwriting an existing key is just another set() — no separate
// "update" API.
store.set("string_key", "new_value")?;
assert_eq!(store.get("string_key"), Some(&b"new_value"[..]));
```

### 6.1. `set_with_ttl` — expiring values

```rust
store.set_with_ttl("session:abc", "user_42", 3600)?; // expires in 1 hour
```

TTL is **absolute Unix wall-clock time, one-second resolution** — not a
monotonic clock. This matters in two ways you should design around:

- A system clock rollback can make an entry live longer than intended;
  a forward clock jump can expire it early. If your application's
  correctness depends on precise expiration timing under adversarial
  clock conditions, TTL alone is not sufficient — add your own
  validation at the point of use.
- Sub-second precision doesn't exist: `set_with_ttl(k, v, 1)` expires
  at the next whole-second boundary from now, which could be anywhere
  from just-under to just-over one second later depending on where in
  the current second the call lands.

```rust
// Special case: TTL of exactly 0 is defined as an immediate remove(),
// not "write a value that's already expired." No value record is ever
// written to the log for a zero-TTL call, so this is the efficient way
// to express "delete if present."
store.set_with_ttl("key_to_delete", "ignored", 0)?;
// equivalent to:
store.remove("key_to_delete")?;
```

---

## 7. Deletion & TTL

```rust
pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()>
pub fn purge_expired(&mut self) -> usize
```

### 7.1. `remove`

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
store.set("temp", "value")?;
store.remove("temp")?;
assert_eq!(store.get("temp"), None);

// Removing a key that was never present, or is already gone, is a
// harmless no-op — not an error.
store.remove("never_existed")?;
```

Internally, `remove()` only writes a tombstone record to the log if the
key currently has a *live* (non-expired) entry — removing an already-
expired or absent key doesn't bloat the log with a pointless durable
record, since recovery already treats an expired entry as gone.

### 7.2. `purge_expired`

```rust
let removed_count = store.purge_expired();
println!("removed {removed_count} expired entries from memory");
```

This is an **in-memory-only** operation — it does not write tombstones
to the log for the entries it removes (unlike `remove()`). It exists
purely to reclaim memory occupied by expired-but-not-yet-purged entries;
call it periodically in long-running processes with high TTL churn if
memory usage matters, or rely on [`compact()`](#9-compaction) which
calls it automatically as part of rewriting the log.

### 7.3. Understanding `len()` and expired entries

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
store.set_with_ttl("short_lived", "value", 1)?;

std::thread::sleep(std::time::Duration::from_secs(2));

assert_eq!(store.get("short_lived"), None); // correctly treated as gone
assert_eq!(store.len(), 1);                 // but still counted!

store.purge_expired();
assert_eq!(store.len(), 0);                 // now actually removed
```

If your application logic ever branches on `len()` or `is_empty()` and
you use TTLs, call `purge_expired()` first (or accept that the count
includes not-yet-cleaned-up expired entries — this is deliberate,
documented behavior, not a bug).

---

## 8. Lifecycle & Durability

Mezaleth exposes three distinct levels of "written," and it's important
to pick the right one for your durability requirements — none of them
are automatic beyond what you explicitly call.

```rust
pub fn flush(&mut self) -> Result<()>
pub fn sync(&mut self) -> Result<()>
pub fn close(&mut self) -> Result<()>
```

| Call | Guarantees | Cost |
|---|---|---|
| *(nothing — just `set`)* | Bytes are in Mezaleth's userspace buffer | Cheapest |
| `flush()` | Buffer handed to the OS | Cheap |
| `sync()` | OS asked to persist to physical storage | Expensive |
| `close()` | `flush()` + releases the writer and the lock | — |

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
store.set("important", "data")?;

// If the process crashes right here, this write may or may not survive
// — it's sitting in Mezaleth's own buffer, not even handed to the OS
// yet.

store.sync()?; // now it's durable (to the extent the underlying storage
                // stack honors sync requests — see the caveat below)
```

> **Honesty about `sync()`:** neither `sync_data()` nor `sync_all()` is
> a universal guarantee of physical persistence on every storage stack.
> Some virtualized filesystems, network filesystems, and drives with
> volatile write caches that ignore flush requests can still lose data
> after `sync()` reports success. This library does not — and cannot —
> paper over that; if you need stronger guarantees than your storage
> hardware provides, that's outside what any userspace library can do.

### 8.1. `close()` and the instance state machine

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
store.set("a", "1")?;
store.close()?;

// The instance is now Closed. Further mutation is rejected:
assert!(matches!(store.set("b", "2"), Err(Mezaleth::MezError::Closed)));

// Reads still work fine — Closed only blocks the write path.
assert_eq!(store.get("a"), Some(&b"1"[..]));
```

`close()` is idempotent — calling it again on an already-closed instance
is a harmless `Ok(())`. If the flush inside `close()` itself fails, the
instance moves to the `Failed` state instead (see below), and the
writer is deliberately **not** dropped, so no buffered data is silently
discarded — you can retry `close()` or call `flush()`/`sync()` directly
to inspect the failure.

### 8.2. The `Failed` state

Any I/O error during a write-adjacent operation (`set`, `remove`,
`flush`, `sync`, or a `compact()` step after the point of no return)
moves the instance to a sticky `Failed` state:

```rust
pub fn is_failed(&self) -> bool
```

```rust
match store.set("key", "value") {
    Err(Mezaleth::MezError::Io(_)) => {
        assert!(store.is_failed());
        // Every further mutation now returns FailedStorage, not another
        // attempt — the on-disk state after a partial/failed write is
        // unknown, so Mezaleth refuses to risk interleaving garbage
        // with a subsequent legitimate record.
        assert!(matches!(store.set("other", "x"), Err(Mezaleth::MezError::FailedStorage)));

        // The only way out: close and reopen. Reopening re-runs full
        // recovery against whatever is actually on disk, which safely
        // truncates any partial write.
        store.close().ok(); // best-effort; may itself return an error
        drop(store);
        let store = Mezaleth::open("app.mez")?;
    }
    Ok(()) => { /* normal path */ }
    Err(e) => return Err(e),
}
```

**Design intent:** this is the single most important correctness
property in the library. A storage engine that silently keeps accepting
writes after an I/O error risks producing a log where a later "valid-
looking" record was actually written after — and possibly overlapping
with — an earlier failed write. Mezaleth would rather refuse every
further operation than risk that.

### 8.3. `Drop` behavior

If a `Mezaleth` instance is dropped without an explicit `close()`, its
buffered writer receives a best-effort final flush automatically. Like
any `Drop` implementation, this cannot return a `Result` — a failure
during this implicit flush is unobservable. If you need to *know*
whether the final flush succeeded, call `close()`, `flush()`, or
`sync()` explicitly before the instance goes out of scope.

```rust
{
    let mut store = Mezaleth::open_or_create("app.mez")?;
    store.set("a", "1")?;
    // No explicit close() — Drop attempts a best-effort flush here.
    // Fine for a short-lived script; not recommended if you need a
    // durability guarantee on shutdown.
}
```

---

## 9. Compaction

```rust
pub fn compact(&mut self) -> Result<()>
```

The append-only log accumulates overwritten records, tombstones, and
expired entries over time. `compact()` rewrites the entire file
containing only the current live, non-expired data, in the current (v2)
format — including upgrading a legacy v1 file in the process.

```rust
let mut store = Mezaleth::open_or_create("app.mez")?;
for i in 0..10_000 {
    store.set(format!("key{i}"), format!("value{i}"))?;
    store.set(format!("key{i}"), format!("updated{i}"))?; // log now has 2 records per key
}
// The log on disk is roughly 2x the size of the live data at this point.

store.compact()?;
// Now the log contains exactly one record per live key.
```

### 9.1. What `compact()` guarantees

`compact()` is implemented as an explicit state machine that
distinguishes two failure phases, because they have very different
implications for what's actually on disk:

- **`MezError::CompactionFailedBeforeInstall(io::Error)`** — failure
  occurred before the new file replaced the old one. **The original
  database is completely untouched.** Safe to retry, safe to keep using
  the instance as before.

- **`MezError::CompactionInstalledButIncomplete(io::Error)`** — the
  atomic replacement of the destination file **succeeded** (the new,
  compacted v2 file genuinely is what's on disk now), but a step
  *after* that — reopening the writer — failed. **Your data is safe.**
  This instance, however, is now in the `Failed` state, because its
  in-memory bookkeeping may no longer match the file it's pointing at.
  Close it and reopen the path fresh to pick up the compacted file
  cleanly.

```rust
match store.compact() {
    Ok(()) => println!("compacted successfully"),
    Err(Mezaleth::MezError::CompactionFailedBeforeInstall(e)) => {
        eprintln!("compaction failed, but your database is unchanged: {e}");
    }
    Err(Mezaleth::MezError::CompactionInstalledButIncomplete(e)) => {
        eprintln!("compaction SUCCEEDED on disk, but this handle needs reopening: {e}");
        store.close().ok();
        // reopen `store` from the same path here
    }
    Err(e) => return Err(e),
}
```

### 9.2. Durability of the replacement itself

- **Unix:** the new file replaces the old one via `rename()`, which is
  atomic at the filesystem level, followed by an `fsync()` of the
  parent directory so the rename's directory-entry update is itself
  durable across power loss.
- **Windows:** replacement uses `MoveFileExW` with
  `MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH` — never a
  non-atomic remove-then-rename. Windows has no safe standard-library
  equivalent of "fsync a directory," so directory-entry durability there
  is best-effort, stated honestly as weaker than the Unix guarantee
  rather than claimed to be equivalent.
- **Other platforms:** falls back to a non-atomic remove-then-rename,
  documented as such — never claimed to be atomic.

Unix file permission bits on the original database file are preserved
across compaction (the new file gets the same mode before it's
installed), which matters if your database contains sensitive data and
you've set restrictive permissions on it.

### 9.3. When to call it

`compact()` is **manual and blocking** — Mezaleth does not
auto-compact on any threshold. It's an exclusive, `O(live database
size)` operation on the instance (nothing else can run concurrently on
the same instance while it's in progress, since it takes `&mut self`).
Call it:

- periodically, on a schedule appropriate to your write volume,
- reactively, when a write returns
  [`MezError::NeedsCompaction`](#10-resource-limits) because
  `max_log_bytes` was reached,
- or opportunistically at startup/shutdown if log growth matters more
  than the pause a full rewrite introduces.

---

## 10. Resource Limits

```rust
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_key_len: u32,
    pub max_value_len: u32,
    pub max_record_len: u64,
    pub max_entries: Option<usize>,
    pub max_index_bytes: Option<u64>,
    pub max_log_bytes: Option<u64>,
}
```

`Limits::default()` sets generous but finite per-record caps
(`max_key_len = 1 MiB`, `max_value_len = 64 MiB`) and leaves the four
resource-level caps unbounded (`None`), matching the original
unrestricted behavior for anyone who doesn't opt in.

### 10.1. Building a `Limits` value

```rust
use Mezaleth::Limits;

// Constructor takes the two limits with no sensible unbounded default.
let limits = Limits::new(
    256,           // max_key_len: 256 bytes
    1024 * 1024,   // max_value_len: 1 MiB
)
.with_max_record_len(2 * 1024 * 1024)   // total encoded record, incl. overhead
.with_max_entries(1_000_000)             // cap the in-memory index at 1M keys
.with_max_index_bytes(512 * 1024 * 1024) // ...and at ~512 MiB of estimated RAM
.with_max_log_bytes(10 * 1024 * 1024 * 1024); // reject writes once the log hits 10 GiB

let mut store = Mezaleth::open_or_create_with_limits("app.mez", limits)?;
```

All four builder methods (`with_max_record_len`, `with_max_entries`,
`with_max_index_bytes`, `with_max_log_bytes`) are `const fn` and can be
chained fluently as shown, or applied individually.

### 10.2. What each limit actually checks, and when

| Field | Bounds | Checked | On violation |
|---|---|---|---|
| `max_key_len` | Single key's byte length | Every `set()`; every record during recovery | `RecordTooLarge` |
| `max_value_len` | Single value's byte length | Same | `RecordTooLarge` |
| `max_record_len` | Total encoded record (key + value + fixed overhead, including the v2 CRC) | Same | `RecordTooLarge` |
| `max_entries` | Number of keys in the in-memory index | Before inserting a *new* key (overwrites never count against this) | `NeedsCompaction` |
| `max_index_bytes` | Heuristic estimate of in-memory index size (`key.len() + value.len() + ~48` per entry) | Before every `set()` | `NeedsCompaction` |
| `max_log_bytes` | On-disk log size | Before every append, using a running total — rejected writes never touch disk | `NeedsCompaction` |

```rust
let limits = Limits::new(16, 64);
let mut store = Mezaleth::create_new_with_limits("strict.mez", limits)?;

match store.set("a_key_that_is_definitely_too_long_for_sixteen_bytes", "value") {
    Err(Mezaleth::MezError::RecordTooLarge(violation)) => {
        println!(
            "rejected: {:?} was {} bytes, limit is {}",
            violation.kind, violation.actual, violation.limit
        );
    }
    _ => unreachable!(),
}
```

### 10.3. `NeedsCompaction` — a resource limit, not an error

Unlike `RecordTooLarge` (a single record fundamentally can't fit),
`NeedsCompaction` means "the store as a whole is over budget, and
compacting would likely bring it back under" — it's a policy signal,
not a data problem:

```rust
loop {
    match store.set(&key, &value) {
        Ok(()) => break,
        Err(Mezaleth::MezError::NeedsCompaction) => {
            store.compact()?; // reclaim space from overwrites/tombstones/expired entries
            // retry the same set() on the next loop iteration
        }
        Err(e) => return Err(e.into()),
    }
}
```

A call that returns `NeedsCompaction` is guaranteed to have written
nothing — neither to disk nor to the in-memory index — so it's always
safe to retry after compacting, with no risk of a partial or duplicate
write.

---

## 11. Error Handling

```rust
pub type Result<T> = std::result::Result<T, MezError>;

#[derive(Debug)]
pub enum MezError {
    Io(std::io::Error),
    InvalidFormat,
    CorruptedRecord { offset: u64 },
    RecordTooLarge(SizeLimitViolation),
    Closed,
    LegacyReadOnly,
    FailedStorage,
    AlreadyOpen,
    NeedsCompaction,
    CompactionFailedBeforeInstall(std::io::Error),
    CompactionInstalledButIncomplete(std::io::Error),
}
```

`MezError` implements `std::error::Error` (with `source()` correctly
chaining to the underlying `io::Error` for every variant that wraps
one) and `Display`, so it composes normally with `?`, `anyhow`,
`thiserror`, and friends.

### 11.1. Variant-by-variant guide

| Variant | Meaning | What you can do |
|---|---|---|
| `Io(e)` | An underlying OS-level I/O error not covered by a more specific variant | Inspect `e.kind()`; usually a genuine environment problem (permissions, disk full, etc.) |
| `InvalidFormat` | Bad magic bytes, unknown version, invalid flag bits, or a structurally impossible record | The file isn't a valid Mezaleth database, or is corrupted beyond recovery |
| `CorruptedRecord { offset }` | A structurally complete v2 record whose CRC doesn't match | Data corruption at that byte offset; not silently discarded — you must decide how to handle it |
| `RecordTooLarge(v)` | A key/value/record exceeded configured [`Limits`](#10-resource-limits) | Inspect `v.kind`, `v.actual`, `v.limit`, `v.offset` for diagnostics |
| `Closed` | Write attempted after `close()` succeeded | Reopen the database |
| `LegacyReadOnly` | Write attempted on an un-compacted v1 file | Call `compact()` first |
| `FailedStorage` | Instance is in the sticky `Failed` state after an earlier I/O error | Close and reopen — see [§8.2](#82-the-failed-state) |
| `AlreadyOpen` | Another instance already holds the exclusive lock | Wait, or use a different path |
| `NeedsCompaction` | A configured resource limit would be exceeded | Call `compact()`, then retry — see [§10.3](#103-needscompaction--a-resource-limit-not-an-error) |
| `CompactionFailedBeforeInstall(e)` | Compaction failed before replacing the file | Original file untouched; safe to retry |
| `CompactionInstalledButIncomplete(e)` | Compaction's file replacement succeeded, but reopening failed afterward | Data is safe; reopen the path fresh |

### 11.2. `SizeLimitViolation` and `LimitKind`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitKind {
    KeyLen,
    ValueLen,
    RecordLen,
}

#[derive(Debug, Clone, Copy)]
pub struct SizeLimitViolation {
    pub kind: LimitKind,
    pub actual: u64,
    pub limit: u64,
    pub offset: Option<u64>, // Some(...) during recovery; None for a fresh set()
}
```

Both types implement `Display`; `SizeLimitViolation`'s output reads
naturally in logs:

```rust
match store.set(huge_key, "value") {
    Err(Mezaleth::MezError::RecordTooLarge(v)) => {
        log::warn!("write rejected: {v}");
        // e.g.: "key length 5000 exceeds configured limit 256"
    }
    _ => {}
}
```

### 11.3. A complete error-handling example

```rust
use Mezaleth::{Mezaleth, MezError};

fn write_with_retry(
    store: &mut Mezaleth,
    key: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match store.set(key, value) {
        Ok(()) => Ok(()),
        Err(MezError::NeedsCompaction) => {
            store.compact()?;
            store.set(key, value)?; // safe: NeedsCompaction never wrote anything
            Ok(())
        }
        Err(MezError::RecordTooLarge(v)) => {
            Err(format!("value rejected: {v}").into())
        }
        Err(MezError::FailedStorage) => {
            Err("store is in a failed state — restart required".into())
        }
        Err(e) => Err(e.into()),
    }
}
```

---

## 13. Concurrency & Locking Model

- **One process, one instance, one lock.** Every constructor acquires
  an exclusive advisory lock on a `<path>.lock` sidecar file (`flock` on
  Unix, `LockFileEx` on Windows) and holds it for the instance's entire
  lifetime, released on `close()`/`Drop`.
- **A second `open()` on the same path — same process or a different
  one — fails with `MezError::AlreadyOpen`** rather than silently
  corrupting the database or racing with the first instance.
- **This is advisory locking**: it protects cooperating Mezaleth
  instances from each other. It does not protect against a process that
  opens and writes to the raw file directly, bypassing the library.

```rust
let _first = Mezaleth::open_or_create("app.mez")?;
let second = Mezaleth::open("app.mez");
assert!(matches!(second, Err(Mezaleth::MezError::AlreadyOpen)));
```

```rust
use std::sync::{Arc, Mutex};

// Multi-threaded access to ONE instance: wrap it.
let store = Arc::new(Mutex::new(Mezaleth::open_or_create("app.mez")?));

let store2 = Arc::clone(&store);
std::thread::spawn(move || {
    store2.lock().unwrap().set("from_thread", "value").unwrap();
});
```

---

## 14. Common Recipes

### 14.1. Startup: open-or-create with production limits

```rust
use Mezaleth::{Limits, Mezaleth};

fn open_production_store(path: &str) -> Mezaleth::Result<Mezaleth> {
    let limits = Limits::new(4 * 1024, 16 * 1024 * 1024) // 4 KiB keys, 16 MiB values
        .with_max_entries(10_000_000)
        .with_max_log_bytes(50 * 1024 * 1024 * 1024); // 50 GiB before requiring compaction

    Mezaleth::open_or_create_with_limits(path, limits)
}
```

### 14.2. Graceful shutdown

```rust
fn shutdown(mut store: Mezaleth::Mezaleth) {
    match store.close() {
        Ok(()) => log::info!("database closed cleanly"),
        Err(e) => log::error!("final flush failed on shutdown: {e}"),
    }
}
```

### 14.3. Periodic background compaction

```rust
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn spawn_compaction_task(store: Arc<Mutex<Mezaleth::Mezaleth>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(3600));
        let mut guard = store.lock().unwrap();
        if let Err(e) = guard.compact() {
            log::error!("background compaction failed: {e}");
        }
    });
}
```

### 14.4. Storing structured data (with `serde`)

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
}

fn save_user(store: &mut Mezaleth::Mezaleth, user: &User) -> Mezaleth::Result<()> {
    let key = format!("user:{}", user.id);
    let value = serde_json::to_vec(user).expect("serialization cannot fail here");
    store.set(key, value)
}

fn load_user(store: &Mezaleth::Mezaleth, id: u64) -> Option<User> {
    let key = format!("user:{id}");
    store.get(key).and_then(|bytes| serde_json::from_slice(bytes).ok())
}
```

### 14.5. Migrating a legacy v1 database

```rust
fn upgrade_if_needed(path: &str) -> Mezaleth::Result<()> {
    let mut store = Mezaleth::open(path)?;
    if store.is_legacy_read_only() {
        println!("upgrading {path} from v1 to v2...");
        store.compact()?;
        println!("done — {path} is now v2 and writable");
    }
    Ok(())
}
```

---

## 15. Full API Index

### Types

| Type | Purpose |
|---|---|
| `Mezaleth` | The database handle |
| `Limits` | Configurable size/resource policy |
| `LimitKind` | Which limit a `SizeLimitViolation` names |
| `SizeLimitViolation` | Structured detail for `RecordTooLarge` |
| `MezError` | The crate's error type |
| `Result<T>` | `= std::result::Result<T, MezError>` |
| `FxHasher` | The index's internal hasher (public for `BuildHasherDefault`, not typically constructed directly) |

### Constructors

| Function | Existing file | Missing file |
|---|---|---|
| `Mezaleth::open_or_create(path)` | Opens | Creates |
| `Mezaleth::open_or_create_with_limits(path, limits)` | Opens | Creates |
| `Mezaleth::open(path)` | Opens | Errors |
| `Mezaleth::open_with_limits(path, limits)` | Opens | Errors |
| `Mezaleth::create_new(path)` | Errors | Creates |
| `Mezaleth::create_new_with_limits(path, limits)` | Errors | Creates |
| `Mezaleth::create_truncate(path)` | **Destroys, recreates** | Creates |
| `Mezaleth::create_truncate_with_limits(path, limits)` | **Destroys, recreates** | Creates |
| `Mezaleth::create(path)` *(deprecated)* | alias for `create_truncate` | |
| `Mezaleth::newstorage(path)` *(deprecated)* | alias for `create_truncate` | |

### Instance methods

| Method | Signature | Category |
|---|---|---|
| `set` | `fn set<K, V>(&mut self, key: K, value: V) -> Result<()>` | Write |
| `set_with_ttl` | `fn set_with_ttl<K, V>(&mut self, key: K, value: V, ttl_secs: u64) -> Result<()>` | Write |
| `new` *(deprecated)* | alias for `set`/`set_with_ttl` | Write |
| `get` | `fn get<K>(&self, key: K) -> Option<&[u8]>` | Read |
| `contains` | `fn contains<K>(&self, key: K) -> bool` | Read |
| `len` | `fn len(&self) -> usize` | Read |
| `is_empty` | `fn is_empty(&self) -> bool` | Read |
| `remove` | `fn remove<K>(&mut self, key: K) -> Result<()>` | Delete |
| `delete` *(deprecated)* | alias for `remove` | Delete |
| `purge_expired` | `fn purge_expired(&mut self) -> usize` | Delete / maintenance |
| `flush` | `fn flush(&mut self) -> Result<()>` | Durability |
| `sync` | `fn sync(&mut self) -> Result<()>` | Durability |
| `close` | `fn close(&mut self) -> Result<()>` | Lifecycle |
| `compact` | `fn compact(&mut self) -> Result<()>` | Maintenance |
| `is_legacy_read_only` | `fn is_legacy_read_only(&self) -> bool` | Introspection |
| `is_failed` | `fn is_failed(&self) -> bool` | Introspection |

### `Limits` builder methods

| Method | Effect |
|---|---|
| `Limits::new(max_key_len, max_value_len)` | Base constructor; other fields default to unbounded |
| `.with_max_record_len(n)` | Cap total encoded record size |
| `.with_max_entries(n)` | Cap in-memory index key count |
| `.with_max_index_bytes(n)` | Cap estimated in-memory index size |
| `.with_max_log_bytes(n)` | Cap on-disk log size |

---

*This document reflects the API as of Mezaleth 0.1.0. If a signature
here ever disagrees with `src/lib.rs`, the source is authoritative —
please file that as a documentation bug.*
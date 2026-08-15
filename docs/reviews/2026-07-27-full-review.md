# DataBucket Review: Full

**Date:** 2026-07-27
**Scope:** the whole repo: `src/**` (21 files), `codegen/**` (13 files), `tools/**` (2 binaries), `tests/**`, `Cargo.toml`, `.github/workflows/rust.yml`, `README.md`. 5344 lines of Rust total.
**Commit:** `5814f75`
**Reviewer slice:** full (sole reviewer for this repo; a sibling agent covered `/Users/revenge/code/WorkTable`, which is this crate's only consumer)

## Summary

- This is the on-disk format layer for WorkTable: page headers, index pages, table-of-contents pages, a space-info page, and the seek/read/write helpers that move them between `tokio::fs::File` and memory. Roughly 2000 lines of that are live; the rest is dead or commented out.
- **The two headline problems are both durability problems.** First, nothing in this crate or in WorkTable ever calls `sync_all`/`sync_data`, and there is no checksum, no magic number, no atomic page swap, and no write-ahead log. A crash between the two `write_all` calls in `persist_page_in_place` leaves a header whose `data_length` describes a body that was never written. Second, every byte that comes off disk is handed to `rkyv::access_unchecked`, so that half-written page is not an error on restart, it is undefined behaviour.
- Those two compose into the worst case for a storage engine: a power cut produces a file that, when reopened, transmutes attacker-or-entropy-controlled bytes into `&ArchivedString` (a relative pointer plus a length) and into `PageType` (a `#[repr(u16)]` enum with 6 valid discriminants out of 65536). rkyv ships a checked `access` with `bytecheck`; it is used in exactly zero places here.
- Not a single test in the repo touches a `File`. There are no async tests at all. `persist_page`, `parse_page`, `persist_pages_batch`, `parse_index_page_utility`, `update_at` and the whole seek family, which is to say the entire purpose of the crate, have zero coverage. What is well tested is `SizeMeasurable`, where the tests genuinely check the estimate against `rkyv::to_bytes(...).len()` over 10000 string lengths. That is the good part of this codebase.
- The 4 GiB cliff is worth calling out on its own: `seek_to_page_start_relatively` computes the target offset in `u32` while its sibling `seek_to_page_start` computes it in `u64`, so `persist_pages_batch` starts writing pages over unrelated data once a file passes 262144 pages.
- The two Critical patterns the sibling agent found in WorkTable are **cleanly absent here**. There is no `unsafe impl Send`/`Sync`, no `UnsafeCell`, no `tokio::spawn`, no discarded `JoinHandle`, and in fact no `Mutex`/`RwLock`/atomic anywhere in the crate. All synchronisation is delegated to the caller via `&mut File`. Details in the concurrency section.
- **Top 3 things to do:** (1) switch every `from_bytes` to `rkyv::access` with `bytecheck` and make `Persistable::from_bytes` return `Result`, (2) add a per-page CRC plus an `fsync` API and make WorkTable call it, (3) put a proptest round-trip and a `cargo-fuzz` target on `from_bytes` before doing anything else, because you currently have no way to know whether a fix worked.

## Findings

### [SEV-1] Every byte read from disk is deserialized with `rkyv::access_unchecked`

- **ID:** `databucket-full-01`
- **Severity:** Critical
- **Category:** Correctness (unsound `unsafe`)
- **Confidence:** High
- **Location:** `src/util/persistable.rs:31,42,53`; `src/page/util.rs:152`; `src/page/index/page.rs:96,170`; `src/page/index/page_for_unsized.rs:81,90,229,310,317,330`; `src/page/index/table_of_contents_page.rs:67`; `codegen/src/persistable/generator/persistable_impl.rs:93,217,243,286,316,346` (which expands at every `#[derive(Persistable)]` site). Full inventory in Appendix A.
- **What:** `Persistable::from_bytes(bytes, version)` is the single entry point for turning file bytes into typed values, and every implementation of it (hand-written and generated) calls `rkyv::access_unchecked`. rkyv's contract for that function is that the bytes are *already known* to be a valid archive of the target type. Here the bytes were just read out of a file that the process does not control and cannot re-derive. rkyv provides `rkyv::access::<T, Error>` which runs `bytecheck` validation and returns `Result`; it is not used anywhere in the crate.
- **Why it matters:** this is unsound and reachable from entirely safe code, with no attacker required, just a bad sector or a crash mid-write (see `databucket-full-02`). Two concrete UB paths:
  - `PageType` is `#[repr(u16)]` with valid discriminants `{0,1,2,3,30,31}`. `parse_general_header` (`src/page/util.rs:148-157`) accesses `ArchivedGeneralHeader` unchecked and then `rkyv::deserialize`s it. A single flipped bit in the type field produces an invalid enum value; constructing or matching on it is instant UB, and the optimizer is entitled to assume the value is one of the six.
  - `ArchivedString`, used for every `String` key in an index page and every entry of `SpaceInfoPage::row_schema`, is a relative pointer plus a length. A corrupt offset makes `rkyv::deserialize` copy from an arbitrary address in the process, and the result is handed back as a normal `String`. That is an out-of-bounds read that turns into a memory disclosure, and it is not caught by any bounds check because there is none to catch it.
  - `parse_general_header` never validates `header.page_type` against the page type the caller asked for (the only such check in the repo is in the commented-out `parse_data_record` at `src/page/util.rs:299`), and never validates `header.data_version` against `DATA_VERSION`. So even a well-formed file, read with the wrong generic parameter, reinterprets one archived type as another.
- **Fix:** change the trait to `fn from_bytes(bytes: &[u8], version: u32) -> Result<Self, Error>` and use `rkyv::access::<_, rkyv::rancor::Error>` everywhere. The derive in `codegen/src/persistable/generator/persistable_impl.rs` mechanically covers most sites once changed there. Add `#[rkyv(derive(...))]`/`bytecheck` bounds where the derive needs them. Then add the two cheap header checks `parse_general_header` is missing: reject `data_version > DATA_VERSION`, and give `parse_page` an expected-`PageType` argument it actually asserts. This is a breaking change to the public trait and will ripple into WorkTable, which is why it is worth doing in one pass rather than site by site.
- **Effort:** L (a day, most of it propagating `Result` through WorkTable)
- **Blast radius:** every `Persistable` impl in both crates; breaking API change.

### [SEV-2] No durability: no fsync, no checksum, no atomic page write, non-atomic header/body split

- **ID:** `databucket-full-02`
- **Severity:** Critical
- **Category:** Correctness (durability)
- **Confidence:** High
- **Location:** `src/page/util.rs:63-75` (`persist_page_in_place`), `43-61` (`persist_page`), `77-95` (`persist_pages_batch`), `121-146` (`update_at`), `src/page/index/mod.rs:39-47` (`persist_index_page_utility`), `src/page/index/page.rs:205-236`, `src/page/index/page_for_unsized.rs:194-217`
- **What:** `rg 'sync_all|sync_data|fsync'` over `src/` returns nothing. The only flush in the whole stack is `File::flush()` in six places in WorkTable (`src/persistence/space/data.rs:122,181`, `src/persistence/space/index/mod.rs:384,504`, `src/persistence/space/index/unsized_.rs:327,448`), and for `tokio::fs::File` that pushes the userspace buffer to the OS, it does not push the page cache to the platter. Separately, `persist_page_in_place` writes the header and the body as two independent `write_all` calls:

  ```rust
  let inner_bytes = page.inner.as_bytes();
  page.header.data_length = inner_bytes.as_ref().len() as u32;
  file.write_all(page.header.as_bytes().as_ref()).await?;   // says "N bytes follow"
  file.write_all(inner_bytes.as_ref()).await?;              // may not happen
  ```

  There is no checksum over either part, no magic number, no generation/LSN counter, no shadow page, and no log. Nothing in the format lets a reader distinguish a complete page from a torn one.
- **Why it matters:** a crash or power loss between those two writes, or in the middle of the second one, leaves a page whose header confidently advertises `data_length` bytes of body that are stale, zero, or half old and half new. On restart `parse_page_in_place` reads exactly that many bytes and feeds them to `access_unchecked` (`databucket-full-01`), so the failure mode is not "corrupt row", it is UB in the recovery path. Even ignoring the UB: `update_at` (`src/page/util.rs:121-146`) does an in-place overwrite of a row in the data file with no old-version copy anywhere, so a torn write there destroys the row with no way to roll back or even to detect it. Sector-atomicity does not save you: `PAGE_SIZE` is 16384 bytes, four times a typical 4 KiB atomic write unit, and index values straddle whatever boundary they land on.
- **Fix:** three separate pieces, in this order.
  1. Add a checksum. Reserve 4 bytes in `GeneralHeader` (currently 28 bytes, of which 2 are rkyv padding per the doc comment at `src/page/mod.rs:49`) for a CRC32C over the body, computed in `persist_page_in_place` and verified in `parse_page_in_place` before any `access`. This alone converts silent corruption into a clean `Err` and is by far the best value per hour spent.
  2. Expose durability. Add `pub async fn sync(file: &mut File) -> eyre::Result<()>` wrapping `sync_data`, and a `persist_pages_batch_durable` that syncs once at the end of the batch. Then decide, once, in WorkTable, where the sync points are. Right now neither crate owns that decision and so nobody makes it.
  3. Decide on atomicity for the page write itself. The cheap version is a double-write buffer: append the page to a scratch region, `sync_data`, then write it in place. The expensive version is a real WAL. Either needs design discussion; (1) and (2) do not.
- **Effort:** (1) M, (2) S, (3) XL
- **Blast radius:** (1) changes the on-disk format and needs a `DATA_VERSION` bump plus a migration path (note `SpaceInfoPage` already has V1/V2 migration machinery at `src/page/space_info.rs:13-86` to copy). (2) is additive.

### [SEV-3] Length fields read from the file drive allocation and reads with no validation

- **ID:** `databucket-full-03`
- **Severity:** High
- **Category:** Security / Correctness
- **Confidence:** High
- **Location:** `src/page/util.rs:180-189` (`parse_page_in_place`), `src/page/util.rs:314-324` (`parse_space_info`), `src/page/index/page.rs:101-110`, `src/page/index/page_for_unsized.rs:86-108`, `src/page/index/page_for_unsized.rs:325-336`
- **What:** parsing a persisted file is an untrusted-input surface and is not treated as one anywhere. Four distinct shapes:
  - `parse_page_in_place`: `let mut buffer: Vec<u8> = vec![0u8; length as usize];` where `length` is `header.data_length`, a `u32` straight off disk. Up to 4 GiB allocated per page, from a 4-byte field, with no upper bound and specifically no check that `data_length <= INNER_PAGE_SIZE` (16356). A page that claims more than `INNER_PAGE_SIZE` reads straight through its successors and then deserializes the mixture.
  - `parse_space_info`: same, `vec![0u8; header.data_length as usize]`, on the very first page of the file, so it is the first thing an attacker-supplied `.wt` file gets to do.
  - `parse_index_page_utility` (both sized and unsized): reads `size`/`slots_size`/`node_id_size` from the file, feeds them to `persisted_size(...)`, and allocates and reads that many bytes. `slots_size` is a `u16` so the blast is bounded, but `persisted_size` multiplies it by the per-slot size with no overflow check.
  - `UnsizedIndexPage::from_bytes:327`: `let offset = bytes.len() - *offset as usize;` where `offset` comes from the file-supplied slot table. If a slot offset exceeds the buffer length this underflows: a panic in debug, a wrap to a huge value in release, followed by `&bytes[offset..(offset + len)]` which panics. Then `access_unchecked` on whatever slice survives.
- **Why it matters:** memory-exhaustion DoS from a 4-byte field, cross-page reads that silently produce wrong data, and panics in a library that a database calls while holding locks. Combined with `databucket-full-01`, the over-long read is also the mechanism that gets adjacent-page bytes into an unchecked archive access.
- **Fix:** validate at the boundary, once, in `parse_general_header`: reject `data_length as usize > INNER_PAGE_SIZE`. Then use `checked_sub`/`checked_add` for every offset derived from file data in `UnsizedIndexPage::from_bytes` and return `Err` rather than indexing. Mechanical.
- **Effort:** S
- **Blast radius:** `src/page/util.rs` plus `page_for_unsized.rs`; no API change if `from_bytes` already returns `Result` after finding 01.

### [SEV-4] `u32` overflow in `seek_to_page_start_relatively` corrupts files larger than 4 GiB

- **ID:** `databucket-full-04`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/page/util.rs:103-110`, compare with `src/page/util.rs:97-101`
- **What:** the two seek helpers compute the same quantity in different integer widths.

  ```rust
  // correct, u64 throughout
  pub async fn seek_to_page_start(file: &mut File, index: u32) -> eyre::Result<()> {
      file.seek(SeekFrom::Start(index as u64 * PAGE_SIZE as u64)).await?;
  }
  // wrong, multiplication happens in u32
  async fn seek_to_page_start_relatively(file: &mut File, index: u32) -> eyre::Result<()> {
      let curr_position = file.stream_position().await?;
      file.seek(SeekFrom::Current((index * PAGE_SIZE as u32) as i64 - curr_position as i64)).await?;
  }
  ```

  `PAGE_SIZE` is `4096 * 4 = 16384`, so `index * 16384` overflows `u32` at `index >= 262144`, which is exactly 4 GiB into the file.
- **Why it matters:** in debug builds this panics inside a write path. In release builds it wraps silently and the file cursor lands at the wrong absolute offset, after which `persist_pages_batch` writes a full page over unrelated data, and `parse_pages_batch` reads a page from the wrong place and hands it to `access_unchecked`. `persist_pages_batch` has three call sites in WorkTable. This is a data-loss bug that only appears once a table gets big, which is the worst time to find it.
- **Fix:** `(index as u64 * PAGE_SIZE as u64) as i64 - curr_position as i64`. One line. While there, note that the relative-seek dance exists only to save a syscall over an absolute seek; `stream_position` is itself a syscall, so it saves nothing. Simplest correct fix is to delete the function and call `seek_to_page_start`.
- **Effort:** S
- **Blast radius:** `persist_pages_batch`, `parse_pages_batch`, `parse_data_pages_batch`. No API change.

### [SEV-5] Nothing checks that a page's serialized body fits in a page

- **ID:** `databucket-full-05`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/page/util.rs:63-75`; the growth sources are `src/page/space_info.rs:105-135` and `src/page/index/table_of_contents_page.rs:89-95`
- **What:** `persist_page_in_place` writes `inner_bytes` at the page offset with no check that `inner_bytes.len() <= INNER_PAGE_SIZE`. Several inner types have unbounded serialized size:
  - `SpaceInfoPage` (page 0) carries `row_schema: Vec<(String, String)>`, `primary_key_fields: Vec<String>`, `secondary_index_types: Vec<(String, String)>`, and `empty_links_list: Vec<Link>`. The last of those grows by 12 bytes per deleted row and is never truncated by anything in this crate.
  - `TableOfContentsPage` tracks `estimated_size` and exposes `estimated_size()` so the caller can decide when to spill to a new page, but nothing in DataBucket enforces it, and there is no assertion at the write.
- **Why it matters:** once `SpaceInfoPage` passes 16356 bytes it writes straight over page 1 (the first index page). Silent, unrecoverable, and it happens as a function of accumulated deletes rather than of anything the operator did. The only test near this is `src/page/space_info.rs:169-183`, which asserts `bytes.as_ref().len() < INNER_PAGE_SIZE` for a space info with empty vectors, which is the one input that cannot fail.
- **Fix:** in `persist_page_in_place`, `if inner_bytes.as_ref().len() > INNER_PAGE_SIZE { return Err(...) }`. That converts silent corruption into a loud error today; the real fix (spilling `empty_links_list` to its own page chain) is a design question for WorkTable. Do the check now regardless.
- **Effort:** S for the check, L for the spill design
- **Blast radius:** every writer; turns a silent path into a fallible one, which is the point.

### [SEV-6] rkyv's alignment requirement is violated by the buffers passed to `access_unchecked`

- **ID:** `databucket-full-06`
- **Severity:** High
- **Category:** Correctness (unsound `unsafe`)
- **Confidence:** High (that the contract is violated); Medium (that it miscompiles today on x86-64/aarch64)
- **Location:** `src/page/util.rs:149-152`; `src/page/index/page.rs:94-100`; `src/page/index/page_for_unsized.rs:79-94,308-319`; `src/page/index/table_of_contents_page.rs:66-69`; `src/util/persistable.rs:31,42,53`; `codegen/.../persistable_impl.rs:93,217`
- **What:** rkyv requires the byte buffer to be aligned to the archived type's alignment, and only `debug_assert!`s it (`rkyv-0.8.15/src/api/mod.rs:52-66`: "unaligned buffer, expected alignment {} but found alignment {}"). The clearest violation is `parse_general_header`:

  ```rust
  let mut buffer = [0; GENERAL_HEADER_SIZE];   // [u8; 28], alignment 1
  file.read_exact(&mut buffer).await?;
  let archived = unsafe { rkyv::access_unchecked::<<GeneralHeader as Archive>::Archived>(&buffer[..]) };
  ```

  `ArchivedGeneralHeader` needs 4-byte alignment; a stack `[u8; 28]` guarantees 1. The `Vec<u8>` cases are less bad in practice (the system allocator hands back 8- or 16-aligned blocks) but are equally unguaranteed by the language.
- **Why it matters:** it is UB by rkyv's stated contract and by Rust's reference-validity rules, so it is a miscompile waiting for a compiler version or a target that cares. What makes this worth fixing rather than shrugging at is that **the codebase already knows the answer and applies it inconsistently**: `src/page/index/page.rs:165-170` and four of the six generated paths copy into `rkyv::util::AlignedVec::<4>` first. The header path and the TOC path just do not.
- **Fix:** read directly into an `AlignedVec<4>` (or copy into one, matching `page.rs:167`) at every remaining site. Better: once `databucket-full-01` moves everything to checked `rkyv::access`, the alignment precondition is still yours to uphold, so do both in the same pass. Mechanical.
- **Effort:** S
- **Blast radius:** internal only.

### [SEV-7] Zero tests exercise file I/O; no property or fuzz testing of the format round-trip

- **ID:** `databucket-full-07`
- **Severity:** High
- **Category:** Maintainability / Correctness
- **Confidence:** High
- **Location:** all `#[cfg(test)]` modules; `tests/mod.rs`; `tests/data/table.wt`
- **What:** the crate's entire reason to exist is moving pages between a `File` and memory, and no test opens a file. There are no `#[tokio::test]`s at all. Untested: `persist_page`, `persist_page_in_place`, `persist_pages_batch`, `parse_page`, `parse_pages_batch`, `parse_data_page`, `parse_data_pages_batch`, `parse_general_header`, `parse_general_header_by_index`, `parse_space_info`, `update_at`, `seek_by_link`, `seek_to_page_start`, `seek_to_page_start_relatively`, `IndexPage::{read_value_with_index, persist_value, remove_value}`, `UnsizedIndexPage::{read_value_with_offset, persist_value}`, and both `parse_index_page_utility` impls. Every High and Critical finding above lives in that list. `tests/data/table.wt` (112 KB) is an orphan: the only tests that ever read it are in the disabled `src/page/iterators.rs`.

  What *is* tested, and tested well, is `SizeMeasurable`: `src/util/sized.rs:326-351` checks `s.aligned_size() == rkyv::to_bytes(&s).len()` for 10000 string lengths. That is a real oracle-based test and the rest of the suite should look like it.

  There is no property testing and no fuzzing anywhere in the repo (no `proptest`, no `quickcheck`, no `fuzz/` directory, no `arbitrary`).
- **Why it matters:** the specific high-risk untested behaviour is the recovery path: `parse_page_in_place` on a file that was truncated or torn mid-write. That path is currently UB (findings 01, 02, 03) and there is no test that would notice when someone fixes it or when someone breaks it again.
- **Fix:** four tests, in priority order.
  1. **Fuzz the parser.** `cargo fuzz add parse_page`, then
     ```rust
     fuzz_target!(|data: &[u8]| {
         let _ = SpaceInfoPage::<()>::from_bytes(data, 2);
         let _ = TableOfContentsPage::<(u64, Link)>::from_bytes(data, 0);
         let _ = UnsizedIndexPage::<String, 4096>::from_bytes(data, 0);
     });
     ```
     Today this finds UB in minutes, which is the point: it is the acceptance test for finding 01. It only becomes meaningful once `from_bytes` returns `Result`, so land 01 and this together.
  2. **Torn-write test.** Persist a page to a tempfile, then for every truncation length `0..PAGE_SIZE`, reopen and assert `parse_page` returns `Err` and does not panic. Same loop with a single byte flipped at each offset, asserting `Err` once the CRC from finding 02 exists.
  3. **Property round-trip.** With `proptest`, for arbitrary `IndexPage<u64>` and `UnsizedIndexPage<String, 4096>` states reachable through `apply_change_event`: `from_bytes(as_bytes(p)) == p`, and `as_bytes(p).len() <= DATA_LENGTH`. The second half is the assertion that would have caught finding 05 and the `page_for_unsized.rs:298` underflow.
  4. **Boundary test for finding 04.** On a sparse tempfile, `persist_pages_batch` with page ids straddling 262144 and assert the bytes land where the ids say.
- **Effort:** M for 1 and 2, M for 3
- **Blast radius:** test-only; adds `proptest` and `tempfile` dev-dependencies.

### [SEV-8] Integer overflow and underflow reachable from ordinary operation

- **ID:** `databucket-full-08`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** High
- **Location:** table below
- **What:** a panic inventory. In debug these panic; in release they wrap and produce wrong offsets, which is worse.

  | Site | Expression | Trigger |
  |---|---|---|
  | `src/page/data.rs:21,31,39,49` | `link.offset + link.length` | `u32` overflow makes the bounds check pass, then `start..end` with `end < start` panics |
  | `src/page/util.rs:134` | `(link.offset + link.length) > DATA_LENGTH` | same, but the consequence is a write at the wrong file offset |
  | `src/page/index/page.rs:151` | `self.current_length - index as u16` | `split(index)` with `index > current_length` |
  | `src/page/index/page.rs:154` | `self.slots[index - 1]` | `split(0)` |
  | `src/page/index/page.rs:227` | `size as u16 - 1` | `size == 0` |
  | `src/page/index/page_cdc_impl.rs:58` | `self.current_length as usize - 1` | `RemoveAt` on an empty page |
  | `src/page/index/page_cdc_impl.rs:109` | `self.current_length -= 1` | `RemoveAt` on an empty page |
  | `src/page/index/page_for_unsized_cdc_impl.rs:59,103` | `self.slots_size as usize - 1`, `self.slots_size -= 1` | same |
  | `src/page/index/page_for_unsized.rs:298` | `data_length - *offset as usize` | page overflow (finding 05) turns into a panic inside `as_bytes` |
  | `src/page/index/table_of_contents_page.rs:109,130` | `self.estimated_size -= ...` | `estimated_size` drifting below the subtrahend; the add and subtract paths use different formulas (`insert` uses `(val, page_id).aligned_size()`, `remove_without_record` uses `align(val.aligned_size() + PageId::default().0.aligned_size())`) so drift is expected, not hypothetical |
  | `src/page/index/table_of_contents_page.rs:135` | `.expect("value should be available if remove is called")` | removing a key that is not present |
  | `src/page/index/page_for_unsized.rs:131,151,170,177` | `... as u16` | truncation if a single index value exceeds 65535 bytes |
- **Why it matters:** this is a library that a database calls, in many cases while holding a lock. A panic there is at best a poisoned lock and at worst a lost write that the caller believes succeeded.
- **Fix:** `checked_add`/`checked_sub` returning `Err` on the file-facing paths (`data.rs`, `util.rs:134`, `page_for_unsized.rs:298,327`); `debug_assert!` plus an early return on the internal invariant paths (the CDC apply functions). Mechanical but touches many lines.
- **Effort:** M
- **Blast radius:** internal; a few signatures gain `Result`.

### [SEV-9] `DataPage::from_bytes` panics on almost every input and does not round-trip with `as_bytes`

- **ID:** `databucket-full-09`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/page/data.rs:54-67`
- **What:**
  ```rust
  fn as_bytes(&self) -> impl AsRef<[u8]> { &self.data[..self.length as usize] }   // len == self.length
  fn from_bytes(bytes: &[u8], _v: u32) -> Self {
      let mut data = [0; DATA_LENGTH];
      data.copy_from_slice(bytes);                                                 // requires len == DATA_LENGTH
      Self { length: bytes.len() as u32, data }
  }
  ```
  `copy_from_slice` panics unless the lengths match exactly, so `from_bytes(as_bytes(p))` panics for every page that is not exactly full. `length` is also redefined on the way back in: it becomes the byte count read rather than the page's logical high-water mark, so a round trip through a full-size buffer reports `DATA_LENGTH` regardless of how much of the page is live.
- **Why it matters:** the generic `parse_page::<DataPage<N>, _>` path is a panic. The reason nobody has hit it is that `parse_data_page` (`src/page/util.rs:243-265`) bypasses `Persistable` entirely and builds the `DataPage` by hand, taking `length` from the header. That is a second, incompatible deserialization of the same type living ten lines away from the first, and the fact that only one of them is used is luck.
- **Fix:** `data[..bytes.len()].copy_from_slice(bytes)` after a length check, keep `length` from the header, and delete one of the two paths. Small but needs a decision on which is canonical.
- **Effort:** S
- **Blast radius:** `DataPage` only.

### [SEV-10] A short read is silently accepted and zero-fills the rest of a data page

- **ID:** `databucket-full-10`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/page/util.rs:248-259`
- **What:**
  ```rust
  let mut buffer = [0u8; INNER_PAGE_SIZE];
  if header.next_id == 0.into() {
      #[allow(clippy::unused_io_amount)]
      file.read(&mut buffer).await?;      // return value discarded
  } else {
      file.read_exact(&mut buffer).await?;
  }
  let data = DataPage { data: buffer, length: header.data_length };
  ```
  The `read` return value is dropped and the lint that would have caught it is suppressed. The intent is clearly "the last page may be short because `persist_page` never pads a page out to `PAGE_SIZE`", but `read` is permitted to return fewer bytes than are available for any reason, not only at EOF.
- **Why it matters:** on a short read the tail of `buffer` stays zero while `length` still comes from the header, so the caller gets a page that claims to be full and is partly zeros. Rows near the end of the last data page silently read as zeros, which then flows into `parse_archived_row`-style consumers. The suppressed lint makes it look deliberate.
- **Fix:** loop until either the buffer is full or the read returns 0, and set `length` to `min(header.data_length, bytes_actually_read)`. Better, have `persist_page` pad each page to `PAGE_SIZE` so `read_exact` always works and the special case disappears; padding also removes the "file is shorter than page_count * PAGE_SIZE" ambiguity in the format.
- **Effort:** S
- **Blast radius:** `parse_data_page`, `parse_data_pages_batch`.

### [SEV-11] `persistence::data` is ~320 lines of dead raw-pointer code with no callers

- **ID:** `databucket-full-11`
- **Severity:** Medium
- **Category:** AI-smell / Security
- **Confidence:** High
- **Location:** `src/persistence/data/{mod,types,util,rkyv_data}.rs`
- **What:** the module implements a schema-driven walker that parses an rkyv row by advancing a raw `*const u8` field by field. It is publicly exported (`pub mod data`, `pub use types::DataTypeValue`). Its callers: `src/page/iterators.rs` (module commented out at `src/page/mod.rs:4`), the commented-out `parse_data_record` (`src/page/util.rs:290-312`), and `tools/dump-data-file` (does not compile, finding 12). `rg` over WorkTable finds zero uses of `parse_archived_row` or `DataTypeValue`. The unsafe in it is the least defensible in the crate:
  - `src/persistence/data/rkyv_data.rs:22`: `buf.as_ptr().add(buf.len()).sub(data_length)` where `data_length` is derived from a *schema* and `buf` from a *file*. If the schema is wider than the row, the pointer is computed outside the allocation, which is UB before any dereference.
  - `src/persistence/data/types.rs:83`: `unsafe { (*archived_ptr).to_string() }` on a `*const ArchivedString` obtained by walking that pointer. No bounds check at any step; the walk can run off the end of the buffer.
  - `src/persistence/data/types.rs:51-72`: `FromStr` declares `type Err = ()` and then `unreachable!()`s on an unknown type name. So the `Result` is a lie, every caller's `.expect("data type should be supported")` is unreachable, and the actual behaviour on a bad schema string is a panic. Unknown type names come from `SpaceInfoPage::row_schema`, which is read from the file, so this is a file-driven panic.
- **Why it matters:** unsafe code with no callers is unsafe code with no test coverage, no review pressure, and a public export inviting someone to use it. It is also the most likely thing in the repo to be reached for when the dump tool is eventually fixed.
- **Fix:** delete the module, or at minimum make it `pub(crate)` and `#[cfg(feature = "row-inspection")]`. If it is kept, the pointer walk needs a `buf.len()` bound threaded through `advance_pointer` and `from_pointer`, and `FromStr` needs a real error type.
- **Effort:** S to delete, M to make safe
- **Blast radius:** public API removal; nothing in the workspace consumes it.

### [SEV-12] Both `tools/` binaries fail to compile, CI does not build them, and the README documents them as features

- **ID:** `databucket-full-12`
- **Severity:** Medium
- **Category:** Docs / AI-smell
- **Confidence:** High
- **Location:** `tools/create-data-file/src/main.rs`, `tools/dump-data-file/src/main.rs`, `Cargo.toml:2`, `.github/workflows/rust.yml:22-27`, `README.md:5-20`
- **What:** `cargo check -p create-data-file` fails with 7 errors: `IndexData`, `SpaceInfoData`, `Interval` with fields like `primary_key_intervals`, `read_data_pages`, `PageIterator`, `DataIterator` and `LinksIterator` no longer exist in the crate, and the tools call the now-`async` `persist_page` with a `std::fs::File`:
  ```
  error[E0599]: no method named `unwrap` found for opaque type `impl Future<...>`
  error[E0308]: mismatched types: expected `&mut tokio::fs::File`, found `&mut std::fs::File`
  ```
  They have not been touched since the commit that added them (`45596b6`, PR #7); the format moved out from under them in `f11cfdd` ("Move to new persistence model for wt"). CI runs `cargo build` and `cargo clippy --all-targets` at the workspace root, and because the workspace root is *itself* a package, cargo's default members is just the root package, so the tools are never built and CI is green. `README.md` documents both tools including sample output.
- **Why it matters:** the README makes a promise the repo cannot keep, and the CI configuration hides it. Anyone who tries to inspect a `.wt` file (the natural first move when debugging any of the durability findings above) hits this wall immediately.
- **Fix:** either delete `tools/` and its README section, or rewrite the two binaries against the current API. Independently, add `default-members = [".", "codegen", "tools/create-data-file", "tools/dump-data-file"]` to the workspace so CI actually builds what the workspace claims to contain. A working dump tool is genuinely valuable for the rest of this review's fixes, so my recommendation is to fix rather than delete.
- **Effort:** M
- **Blast radius:** tools only, plus a CI config line that will surface any other rot.

### [SEV-13] The derive macro's `String` branch emits code that does not compile, and its ordering validation is a no-op

- **ID:** `databucket-full-13`
- **Severity:** Medium
- **Category:** AI-smell / Correctness
- **Confidence:** High
- **Location:** `codegen/src/persistable/generator/persistable_impl.rs:311-321` and `:168-179`
- **What:** two independent defects in the same function.
  ```rust
  // :312 - missing semicolon; `#size_ident` is also a u16 used where usize is needed
  let values_len = align(#size_ident + 8)
  let mut v = rkyv::util::AlignedVec::<4>::new();
  ```
  Any struct with `#[persistable(by_parts)]` and a `String` field fails to compile with a syntax error pointing into generated code. No struct in the repo takes that branch, so it has never been exercised. Second:
  ```rust
  let mut correct_order = true;
  for i in 0..size_fields.len() {
      correct_order = size_fields.iter().any(|(pos, _)| *pos == i);   // assigns, does not AND
  }
  ```
  Only the last iteration survives, so the "size fields must come first" validation passes for almost any ordering. The single-size-field branch above it (`:154-167`) does its checks correctly, which is what makes the multi-field branch look like it was written by a different hand.
- **Why it matters:** the first is a landmine for the next person to add a `String` field to a by-parts page; the error will point at macro output, not at their struct. The second means the macro will happily generate a `from_bytes` that reads fields in an order that does not match `as_bytes`, producing silent misparsing rather than a compile error, which is the failure mode this validation exists to prevent.
- **Fix:** add the semicolon and an `as usize`; change the loop to `correct_order &= ...`. Then add a `trybuild` case per branch so the generated code is at least compiled once.
- **Effort:** S
- **Blast radius:** codegen only.

### [SEV-14] `SizeMeasurable` for `Vec<T>` and `Option<T>` is wrong for anything but fixed-size `T`

- **ID:** `databucket-full-14`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/util/sized.rs:147-169` and `:177-188`
- **What:** `Vec<T>::aligned_size` computes the per-element size from `T::default().aligned_size()` and multiplies by `self.len()`. For `Vec<String>` that is 8 bytes per element (the `String` impl at `:137-145` returns 8 for anything up to 8 bytes) regardless of the actual contents, so a `Vec` of 100-byte strings is estimated at 8 bytes each. `Option<T>::aligned_size` returns `size_of::<Option<T>>()`, the *native* Rust size, not the archived size; it is right for `Option<f64>` (16 either way, and that is the only case tested, at `:316-323`) and wrong in general, notably for `Option<String>`.
- **Why it matters:** these estimates are what the caller uses to decide when a page is full (`get_index_page_size_from_data_length`, `TableOfContentsPage::estimated_size`). An underestimate is exactly how you overflow a page, which is finding 05. Note the git history already contains "Fix `Option` wrong sizing (#60)", so this area has bitten before.
- **Fix:** for `Vec<T>`, sum `t.aligned_size()` over the elements rather than multiplying a default, and keep the fast path only when `T::align()` proves the type is fixed-size. For `Option<T>`, compute from `T`'s archived size plus the discriminant, and add a test against `rkyv::to_bytes` for `Option<String>` and `Vec<String>` mirroring the excellent `test_string` loop at `:326-335`.
- **Effort:** S
- **Blast radius:** page-fill decisions in both crates; may change how many values fit per page, so it interacts with existing files.

### [SEV-15] API design: stringly-typed errors, `&mut File` cursor contract, no ownership of the file invariant

- **ID:** `databucket-full-15`
- **Severity:** Medium
- **Category:** Design
- **Confidence:** High
- **Location:** whole public surface; representative: `src/page/util.rs:43-146`, `src/lib.rs:12-22`
- **What:** three related shapes.
  - **Errors.** Everything returns `eyre::Result` with `eyre!("...")` strings. There is no error enum, so a caller cannot distinguish "the link is out of bounds" from "the disk is full" from "this page is corrupt" without matching on message text. Once findings 01 and 03 land, "corrupt page" becomes a routine outcome that WorkTable must handle differently from an IO error, and there will be nothing to match on.
  - **The file contract.** Every operation is `seek` then `read`/`write` on a caller-supplied `&mut File`, so correctness depends on the caller holding exclusive access across the *pair*, an invariant that exists only in the author's head. `pread`/`pwrite` (`std::os::unix::fs::FileExt::{read_at, write_at}`) would make each operation a single positional syscall with no shared cursor, removing both the invariant and half the syscalls.
  - **Layering.** Durability policy is currently split across the crate boundary with neither side owning it: DataBucket writes and exposes no sync at all; WorkTable calls `flush()` in six places. That is the concrete answer to "do WorkTable and DataBucket duplicate logic that should live in one place": the *file lifecycle* (open, page allocation, sync points, recovery) is half here and half there. It should be one owned `SpaceFile` type in DataBucket that holds the `File` and exposes page-level operations, with WorkTable holding that instead of a raw `File`.
- **Why it matters:** the missing owner is why findings 02, 04 and 05 all exist. Nobody is responsible for "a page write is complete and durable", so nothing checks it.
- **Fix:** (a) introduce `pub enum Error { Io(std::io::Error), CorruptPage { page_id: PageId, .. }, PageOverflow { .. }, InvalidLink { .. } }` with `thiserror`/`derive_more` and keep `eyre` only at the top of WorkTable; (b) introduce `SpaceFile` owning the `File`, with `read_page`/`write_page`/`sync`, and move the seek helpers behind it using positional I/O. Needs design discussion, and it is the natural umbrella for landing 02 through 05.
- **Effort:** L
- **Blast radius:** breaking for WorkTable; do it in the same pass as finding 01 since that is already breaking.

### [SEV-16] Performance: full-page rewrites, two allocations and a copy per value read, O(n) syscalls per insert

- **ID:** `databucket-full-16`
- **Severity:** Medium
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/page/index/page_for_unsized.rs:281-305`, `src/page/index/page.rs:160-172,220-236`, `src/page/index/table_of_contents_page.rs:53-65`, `src/page/space_info.rs:122-135`, `src/page/util.rs:43-61`
- **What:**
  - `UnsizedIndexPage::as_bytes` allocates a zeroed `Vec` of the full `DATA_LENGTH`, serializes the utility struct, copies it out with `.to_vec()` (a second allocation that exists only to satisfy the borrow checker), then re-serializes *every* index value and `splice`s each one in. That is a complete page rebuild for a one-value change, and `splice` is a general-purpose iterator-based operation where `copy_from_slice` on a same-length range would do. `persist_value` exists for the incremental case, so both strategies are implemented and the expensive one is on the `Persistable` path.
  - `IndexPage::persist_value:227-233` finds the next free slot by reading values back one at a time, each a separate `read_exact` syscall: O(n) syscalls per insert, on a page holding on the order of 500 values for `u64` keys at the default page size.
  - Every value read does `vec![0u8; n]` then `AlignedVec::extend_from_slice` (`page.rs:165-168`, `page_for_unsized.rs:224-227`, plus four generated sites): two allocations and a copy where reading straight into the `AlignedVec` is one allocation and no copy.
  - `TableOfContentsPage::as_bytes` clones the whole `BTreeMap` into a `Vec` on every persist; `SpaceInfoPage::as_bytes` clones all eight fields to build a V2 mirror; both run on every write of those pages.
  - `persist_page` is 5 syscalls per page (seek, write, write, stream_position, seek). Two of those exist only to reposition the cursor for a caller that may not need it.
- **Why it matters:** the index write path is the hot path for a database, and the current shape is "rebuild and rewrite 16 KB per changed value". At a few thousand writes per second that is tens of MB/s of avoidable page traffic before any of it reaches the disk.
- **Fix:** in order of value: read into `AlignedVec` directly (mechanical, no API change); maintain a free-slot cursor in the page header instead of scanning (`current_index` almost is one already, it is just not trusted); make `as_bytes` for the unsized page write into a caller-supplied buffer and only re-serialize dirty slots; replace the `stream_position`/`seek` tail of `persist_page` with positional writes per finding 15.
- **Effort:** M
- **Blast radius:** internal, except the `as_bytes` signature change.

### [SEV-17] Copy-paste slice range in `UnsizedIndexPage::from_bytes`, correct only by coincidence

- **ID:** `databucket-full-17`
- **Severity:** Low
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/page/index/page_for_unsized.rs:313-315`
- **What:**
  ```rust
  let node_id_size_bytes = &bytes[UnsizedIndexPageUtility::<T>::slots_size_size()
      ..UnsizedIndexPageUtility::<T>::node_id_size_size()
          + UnsizedIndexPageUtility::<T>::node_id_size_size()];
  ```
  The end bound should be `slots_size_size() + node_id_size_size()`. It reads `node_id_size_size()` twice. Both fields are `u16` so both functions return 2 and the range `2..4` is right by accident.
- **Why it matters:** it silently starts reading the wrong bytes the moment either size field changes type, and the failure will present as corrupt index pages rather than as a compile error.
- **Fix:** one-line correction.
- **Effort:** S
- **Blast radius:** none today.

### [SEV-18] The proc-macro crate depends on rkyv 0.7, regex, convert_case, scc and lockfree, none of which it uses

- **ID:** `databucket-full-18`
- **Severity:** Low
- **Category:** Maintainability / Supply chain
- **Confidence:** High
- **Location:** `codegen/Cargo.toml:16-27`, `Cargo.toml:26`
- **What:** `data_bucket_derive` is a proc-macro crate; it only emits tokens. Its manifest declares `rkyv = "0.7.45"`, `regex`, and `convert_case` as dependencies and `scc` plus `lockfree` as dev-dependencies. `rg` over `codegen/src` finds `rkyv` only inside `quote!` string output, and finds `regex`, `convert_case`, `scc` and `lockfree` nowhere at all. `cargo tree -i rkyv@0.7.46` confirms the only thing pulling rkyv 0.7 into the build graph is this crate, so every build of DataBucket compiles an entire second major version of rkyv, for the host, for nothing. Separately, the main crate takes `tokio = { version = "1", features = ["full"] }` while using only `fs` and `io-util`, pulling the net, signal, process and time machinery into every downstream binary.
- **Why it matters:** build time and dependency surface, and the presence of a second rkyv major version in the lock file is a trap for anyone grepping for format-compatibility questions. `lockfree` was dropped from the main crate in `a3f02cf` but survives here.
- **Fix:** delete the five unused deps; narrow tokio to `features = ["fs", "io-util"]`. Verify with `cargo check -p data_bucket_derive`.
- **Effort:** S
- **Blast radius:** build only.

### [SEV-19] The documented on-disk header layout does not match the struct

- **ID:** `databucket-full-19`
- **Severity:** Low
- **Category:** Docs
- **Confidence:** High
- **Location:** `src/page/mod.rs:36-50` vs `src/page/header.rs:29-37`
- **What:** the doc comment for `GENERAL_HEADER_SIZE` lists the fields in the order `data_version, page_id, previous_id, next_id, page_type, space_id, data_length`. The struct declares them `data_version, space_id, page_id, previous_id, next_id, page_type, data_length`. `space_id` is in position 6 in the doc and position 2 in reality. The byte total (28) is right, which is why `general_header_length_valid` passes and nobody noticed.
- **Why it matters:** this comment is the only written description of the on-disk header layout in the repo. Anyone writing a reader in another language, or hand-decoding a `.wt` file while debugging finding 02, gets the wrong answer.
- **Fix:** reorder the comment to match. While there, the "2 bytes are added by rkyv implicitly" note is the natural place to record that those 2 bytes are available for the CRC in finding 02.
- **Effort:** S
- **Blast radius:** docs.

### [SEV-20] Dead code inventory

- **ID:** `databucket-full-20`
- **Severity:** Low
- **Category:** AI-smell
- **Confidence:** High
- **Location:** listed below
- **What:**
  - `src/page/iterators.rs`: 225 lines, module commented out at `src/page/mod.rs:4` and re-export commented at `:21`. References `IndexData`, `SpaceInfo`, and a sync `parse_space_info` that no longer exist; would not compile if enabled. Last touched by the commit that broke it (`f11cfdd`).
  - `src/page/util.rs:290-662`: 372 commented-out lines, 56% of the file, containing three commented functions and a full commented test module.
  - `src/space.rs:6-10`: `pub struct Space {}` with its only field commented out; exported nowhere useful, referenced by nothing.
  - `align_vec` (`src/util/sized.rs:25-34`): publicly exported from `src/lib.rs:22`, zero callers in either crate.
  - `PersistableTable` (`src/persistence/table.rs`): a 6-line trait, publicly exported, zero implementors and zero callers in either crate. `PersistableIndex` has two references in WorkTable, so it survives, but both are single-implementor traits in the "invented abstraction" sense.
  - `parse_pages_batch` (`src/page/util.rs:197`): exported, zero callers in WorkTable (its `parse_data_pages_batch` sibling is used).
  - `tests/data/table.wt`: 112 KB fixture, read only by the disabled `iterators.rs` tests.
  - `TODO` inventory is small and honest: two entries, `src/page/mod.rs:30` ("Move to config", about `PAGE_SIZE` being a hardcoded const, which is a real design constraint worth keeping visible) and `src/page/space_info.rs:88`.
- **Why it matters:** the commented-out code is not neutral. `src/page/util.rs` reads as a 662-line file when it is a 290-line file, and the commented tests describe an API that has not existed for many releases, which actively misleads.
- **Fix:** delete. It is all in git history. If `iterators.rs` is wanted, it belongs with the `tools/` fix in finding 12 since they are the same dead subsystem.
- **Effort:** S
- **Blast radius:** removes public items (`align_vec`, `PersistableTable`, `Space`), so it is a semver-breaking cleanup; bundle with finding 15.

## Concurrency: the two WorkTable patterns are absent here

Reporting this explicitly since it was asked for.

- **Pattern (a), `UnsafeCell` behind a hand-written `unsafe impl Sync` with an unlocked read path: absent.** `rg 'unsafe impl|UnsafeCell'` over `src/` and `codegen/` returns nothing. There is no `Send`/`Sync` implemented by hand anywhere in this crate; the `Send + Sync` bounds that appear (`src/page/util.rs:48,68,80`, `src/page/index/mod.rs:28`) are ordinary trait bounds on generic parameters, not assertions. Appendix A lists every `unsafe` block, and all 29 of them are `rkyv::access_unchecked` or raw-pointer arithmetic, none of them shared-state assertions.
- **Pattern (b), detached `tokio::spawn` with a dropped `JoinHandle`: absent.** No `tokio::spawn`, no `JoinHandle`, no channels, no background task of any kind. The crate's async is purely `tokio::fs` I/O.
- **No locks at all.** No `Mutex`, `RwLock`, or atomic in the crate, so there is no lock ordering to get wrong, nothing held across an `await`, and no atomic ordering to audit. All synchronisation is the caller's problem.
- **The real concurrency risk here is the contract, not the code.** Every operation is seek-then-read/write on a `&mut File`. `&mut` gives exclusivity over the *handle*, not over the *file*: two `File` objects on the same path (or a `dup`'d fd) interleave their cursors freely, and the crate has no way to detect it. There is a genuine TOCTOU shape in `parse_index_page_utility` (`src/page/index/page.rs:86-114`), which reads the `size` field, seeks backwards, and then reads `persisted_size(size)` bytes: if anything moved the cursor or rewrote the page between those two reads, the second read is framed by a stale length. Today WorkTable holds one `File` per space file and serialises access, so this is latent rather than live. Positional I/O (finding 15) removes the class entirely.

## Cross-cutting recommendations

1. **Make `from_bytes` fallible and checked, in one breaking pass.** This is findings 01, 03, 06 and half of 08. Change `Persistable::from_bytes` to return `Result`, swap `access_unchecked` for `rkyv::access`, read into `AlignedVec` at every site, and validate `data_length` in `parse_general_header`. Plan: change the trait, fix the derive in `codegen/src/persistable/generator/persistable_impl.rs` (which covers most call sites automatically), then the eight hand-written impls, then propagate through WorkTable's `src/persistence/space/`. What breaks: every `Persistable` implementor in both crates, and WorkTable's call sites gain a `?`. Do this first, because every other correctness fix is unverifiable while the parser is UB on bad input.
2. **Give the format a checksum and the API an fsync, then decide sync points once.** Finding 02. Plan: take 4 of the 28 header bytes for a CRC32C over the body, bump `DATA_VERSION` to 3, reuse the existing V1/V2 migration shape in `src/page/space_info.rs` for the transition, add `SpaceFile::sync`, and have WorkTable call it at transaction boundaries instead of sprinkling `flush()`. What breaks: existing `.wt` files need the migration path; a version-2 file must still be readable.
3. **Own the file: introduce `SpaceFile` and use positional I/O.** Findings 04, 05, 10, 15, part of 16. A type that owns the `File`, knows `PAGE_SIZE`, does `read_at`/`write_at` instead of seek-then-read, checks page bounds on every write, and is the only place page offsets are computed. That deletes `seek_to_page_start_relatively` (and its 4 GiB bug) rather than fixing it, removes the short-read special case, and halves the syscalls. What breaks: WorkTable stops holding a raw `File`.
4. **Put a fuzz target and a proptest round-trip on the format before landing 1 through 3.** Finding 07. This is the only way to know whether the parser rewrite is correct, and the torn-write truncation test is the only way to know whether the checksum works. It is cheap and it is the acceptance criterion for everything above.
5. **Delete the dead subsystem, or fix it and put it in CI.** Findings 11, 12, 20. `persistence::data` + `iterators.rs` + both `tools/` binaries + 372 commented lines in `util.rs` are one coherent dead layer, roughly 900 lines. Either it goes, or it gets fixed and added to `default-members` so CI keeps it alive. Leaving it half-present is the worst of the three. A working `dump-data-file` would materially help with recommendations 1 through 3, so I would fix rather than delete.
6. **One error type.** Finding 15a. `eyre` strings cannot express "this page is corrupt, fall back to the replica" versus "the disk is full". Once the parser is fallible (recommendation 1) the caller needs that distinction immediately, so define the enum in the same pass.

## What I did not cover

- **I did not run the test suite**, only `cargo check --offline --all-targets` and `cargo clippy --offline --all-targets` (both clean, 1.6s, and note that both silently exclude the `tools/` members). The findings about test coverage are from reading every `#[cfg(test)]` module, not from a coverage run.
- **I did not review WorkTable**, beyond targeted greps to answer "does anything call this?" and "who flushes?". Claims about WorkTable usage counts come from `rg` over `WorkTable/src`, `WorkTable/codegen/src` and `WorkTable/tests`, and could miss a macro-generated call site. The sibling agent's review at `WorkTable/docs/reviews/2026-07-27-perf-concurrency-unsafe.md` is the authority there.
- **I did not verify the on-disk format against a real `.wt` file.** `tests/data/table.wt` exists but the tool that would decode it does not compile.
- **I did not audit the `WorkTablesIndex` (indexset) dependency** at `=0.0.1`, which supplies the CDC `ChangeEvent` types that `page_cdc_impl.rs` consumes. Whether `RemoveAt` can legitimately arrive for an empty page (which decides whether finding 08's CDC underflows are reachable or merely defensive) depends on that crate's guarantees, which I did not read.
- **I did not benchmark anything.** The performance findings are structural (allocation counts, syscall counts, big-O) and quantified by inspection, not measured.
- **No security review of the derive macro's expansion under adversarial input**, since proc macros only run at build time on trusted source.

## Quick-start for the follow-up agent

Read in this order:

1. `src/page/util.rs:1-290` (ignore everything below 290, it is commented out). This is the whole I/O layer: persist, parse, seek. Findings 02, 03, 04, 05 and 10 are all in these 290 lines.
2. `src/util/persistable.rs` (56 lines). The `Persistable` trait, which is the seam every fix in recommendation 1 goes through.
3. `codegen/src/persistable/generator/persistable_impl.rs`. The derive that generates most `from_bytes` implementations; changing it changes most call sites at once.
4. `src/page/index/page_for_unsized.rs:268-348`. The most intricate serialization in the crate (values packed from the page tail, utility from the head) and the site of findings 03, 08 and 17.
5. `src/page/header.rs` + `src/page/mod.rs:30-101`. The 28-byte header and the page constants; where a CRC would go.
6. `src/util/sized.rs`. The size-estimation layer, and the only well-tested part of the repo. Read the tests at the bottom as the model for what new tests should look like.

Commands:

```
cargo check --offline --all-targets          # ~2s, clean; does NOT build tools/
cargo clippy --offline --all-targets         # clean, and CI runs it with -D warnings
cargo test --offline                         # unit tests only; no async, no file I/O
cargo check --offline -p create-data-file    # 7 errors, see finding 12
cargo tree --offline -i rkyv@0.7.46          # shows the stray proc-macro dep, finding 18
```

Surprises about the layout and conventions:

- **The workspace root is also a package**, so `cargo build`/`test`/`clippy` at the root build only the root package plus `codegen`. `tools/*` are workspace members that nothing ever builds. This is why CI is green on code that does not compile.
- **`Cargo.lock` is in `.gitignore`** (line 6) and is therefore untracked, while `Cargo.toml` pins `data_bucket_derive` and `indexset` with `=` exact versions. CI does not pass `--locked`. So dependency resolution is fresh on every CI run, which is the "CI installs without a frozen lockfile" pattern from the shared brief, in its Rust form.
- **There is no `CLAUDE.md`, `AGENTS.md`, or `docs/` in this repo** (this review creates `docs/reviews/`). The README covers only the two broken tools. All design intent has to be inferred from the code and from commit messages.
- **`src/page/util.rs` is 662 lines of which 372 are commented out.** Do not judge the file by its length.
- **The crate is published to crates.io** (`data_bucket = "=0.4.0"` in WorkTable's manifest, not a path dependency), so the breaking changes in recommendations 1 and 3 need a version bump and a coordinated WorkTable release, not just a local edit. The manifest keeps commented-out path/git alternatives at `Cargo.toml:22-24` for local iteration.

## Appendix A: every `unsafe` block and whether its invariant is enforced

29 blocks. None of them are `unsafe impl`; all are `access_unchecked` or raw-pointer arithmetic. "Enforced" means something in the code actually establishes the invariant, not that it happens to hold today.

| # | Location | Invariant relied on | Enforced? |
|---|---|---|---|
| 1 | `src/util/persistable.rs:31` `Vec<T>::from_bytes` | bytes are a valid archive of `Vec<T>`; buffer aligned | **No** on both. Bytes come from a file; buffer is a caller `&[u8]`. |
| 2 | `src/util/persistable.rs:42` `u8::from_bytes` | same | **No**. Alignment trivially OK (align 1); validity not checked. |
| 3 | `src/util/persistable.rs:53` `String::from_bytes` | same | **No**. `ArchivedString` is a relative pointer; a bad offset reads arbitrary memory. |
| 4 | `src/page/util.rs:152` `parse_general_header` | valid `ArchivedGeneralHeader`; buffer 4-byte aligned | **No** on both. Buffer is a stack `[u8; 28]` (align 1). `PageType` discriminant unvalidated. Finding 06. |
| 5 | `src/page/index/page.rs:96` | valid archived `u16`; aligned | **No**. `Vec<u8>` buffer, file contents. |
| 6 | `src/page/index/page.rs:170` `read_value` | valid archived `IndexValue<T>`; aligned | Alignment **yes** (`AlignedVec::<4>`). Validity **no**. |
| 7 | `src/page/index/page_for_unsized.rs:81` | valid archived `u16`; aligned | **No**. |
| 8 | `src/page/index/page_for_unsized.rs:90` | valid archived `u16`; aligned | **No**. |
| 9 | `src/page/index/page_for_unsized.rs:229` `read_value` | valid archived `IndexValue<T>`; aligned | Alignment **yes**. Validity **no**. |
| 10 | `src/page/index/page_for_unsized.rs:310` | valid archived `u16`; aligned; slice in bounds | **No**. Slice bound is a `&bytes[0..2]` on caller data. |
| 11 | `src/page/index/page_for_unsized.rs:317` | same | **No**, and the slice range itself is wrong (finding 17). |
| 12 | `src/page/index/page_for_unsized.rs:330` | valid archived `IndexValue<T>` at a file-supplied offset; aligned; in bounds | **No** on all three. Offset is `bytes.len() - slot_offset`, unchecked (finding 03). |
| 13 | `src/page/index/table_of_contents_page.rs:67` | valid archived `TableOfContentsPagePersisted<T>`; aligned | **No**. File contents into a `&[u8]`. |
| 14 | `src/page/space_info.rs:225` | valid archive (test only) | Test-only; input is `to_bytes` output, so **yes** in context. |
| 15 | `src/page/space_info.rs:253` | valid archive (test only) | Test-only; **yes** in context. |
| 16 | `src/persistence/data/rkyv_data.rs:22` | `buf.as_ptr().add(len).sub(data_length)` stays inside the allocation | **No**. `data_length` comes from a schema, `buf` from a file. UB before any read if the schema is wider than the row. **Dead code** (finding 11). |
| 17 | `src/persistence/data/util.rs:13` | both pointers in the same allocation for `byte_offset_from` | **No**. Relies on the caller. Dead. |
| 18 | `src/persistence/data/util.rs:14` | `add` result stays in bounds | **No**. Dead. |
| 19 | `src/persistence/data/types.rs:83` | `*const ArchivedString` valid, aligned, in bounds | Alignment handled manually via `advance_pointer_for_padding`; bounds and validity **no**. Dead. |
| 20 | `src/persistence/data/types.rs:91` | `pointer.add(size_of::<ArchivedString>())` in bounds | **No**. Dead. |
| 21 | `src/persistence/data/types.rs:111` | `*const ArchivedU32` etc valid, aligned, in bounds | Alignment manual; bounds **no**. Dead. Expands 12 times via `impl_datatype!`. |
| 22 | `src/persistence/data/types.rs:127` | `pointer.add(...)` in bounds | **No**. Dead. |
| 23 | `src/page/iterators.rs:49` | valid archived `IndexData<T>` | **No**. Module is commented out of the build (finding 20). |
| 24 | `codegen/.../persistable_impl.rs:93` (generated, full-row) | valid archive; aligned | **No** on both. Expands at `GeneralHeader`, `SpaceInfoPageV1`, `SpaceInfoPageV2`. |
| 25 | `codegen/.../persistable_impl.rs:217` (generated, size field) | valid archived size type; aligned; `bytes[offset..offset+size_length]` in bounds | **No**. Slice can panic on short input. |
| 26 | `codegen/.../persistable_impl.rs:243` (generated, primitive) | valid archive; aligned | Alignment **yes** (`AlignedVec::<4>`). Validity **no**. |
| 27 | `codegen/.../persistable_impl.rs:286` (generated, `Vec`) | valid archive; aligned; `values_len` from a file-read size field | Alignment **yes**. Validity and bounds **no**. |
| 28 | `codegen/.../persistable_impl.rs:316` (generated, `String`) | same | Alignment **yes**. Validity **no**. **This branch does not compile** (finding 13). |
| 29 | `codegen/.../persistable_impl.rs:346` (generated, unsized generic) | same | Alignment **yes**. Validity and bounds **no**. |

Summary: of 29 blocks, 2 are test-only and sound in context, 8 are dead code, and the remaining 19 all rely on "the bytes on disk are a valid archive of exactly this type", which nothing establishes. That is finding 01, and it is one fix.

<details>
<summary>Nits</summary>

- `gen_perisistable_impl`, `gen_perisistable_full`, `gen_perisistable_by_parts`, `gen_perisistable_by_parts_as_bytes_fn`, `gen_perisistable_by_parts_from_bytes_fn`: "perisistable" is misspelled in five function names in `codegen/src/persistable/generator/persistable_impl.rs`.
- `codegen/src/persistable/parser.rs:36`: `.expect("always ok even on unrecognized attrs")` is wrong. `parse_nested_meta` returns `Err` on malformed attribute *syntax*, so a typo in `#[persistable(...)]` panics the compiler with a message asserting it cannot happen.
- `codegen/src/{size_measure,variable_size_measure,persistable}/parser.rs` contain three byte-identical `Parser::parse_struct` implementations; `size_measure/mod.rs` and `variable_size_measure/mod.rs` are byte-identical except for the module path.
- `src/persistence/data/types.rs:69`: `_ => unreachable!()` inside a `FromStr` whose `Err` type is `()`. Either the `Result` or the `unreachable!` is wrong; both cannot be right.
- `src/page/mod.rs:30` `// TODO: Move to config` on `PAGE_SIZE`: worth keeping, since `PAGE_SIZE` being a compile-time const means two binaries built with different values silently produce mutually unreadable files, with nothing in the header recording which was used.
- `src/page/index/page.rs:36-37`: `slots_vec_size` and `index_values_vec_size` are both assigned `IndexPage::<T>::slots_size(0)`. The second looks like it wants `index_values_size(0)`. Both are 8 for an empty vec so the arithmetic works out, but the second name does not mean what it says.
- `src/page/index/page.rs:329`: a stray `println!("size: {size}")` in `test_bytes_128`.
- `src/page/space_info.rs:88`: `// TODO: Minor. Add some schema description in \`SpaceIndo\`` (typo, `SpaceInfo`).
- `src/lib.rs:1`: `extern crate core;` is a 2015-edition artifact, unnecessary in edition 2021.
- `src/page/space_info.rs`: `SpaceInfoPage` has both a `version` field (table schema version) and a `data_version` in the surrounding `GeneralHeader` (format version), and `From<SpaceInfoPageV1>` sets `version: 0`. Two different meanings of "version" one struct apart; worth a doc comment saying which is which.
- `README.md:19-38`: the sample `dump-data-file` output shows a table the tool cannot currently produce (finding 12).
- `src/link.rs:28-35`: `impl<T: AsRef<Link>> PartialEq<T> for Link` calls `other.as_ref().eq(self)`, relying on the derived `PartialEq<Link>`; correct, but one accidental `AsRef<Link> for Link` away from infinite recursion.

</details>

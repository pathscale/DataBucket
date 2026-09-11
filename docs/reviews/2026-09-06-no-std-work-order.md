> Historical review retained from local branches during the September 2026 release audit. This records findings at the original revision, not the current v3 implementation. See [the release reconciliation](2026-09-11-reconciliation.md) before acting on it.

# Splitting the format layer out, and taking it `no_std`

A work order. Everything below marked **measured** was run on 6 September 2026 on
this machine; everything marked **unverified** is a design decision still open.
Read the corrections section before you start, because it undoes claims that
were made confidently and were wrong.

## Why anyone wants this

**Not for `WorkTable-vec`.** That was the original reason and it did not
survive contact. `WorkTable-vec` is `#![no_std]` and alloc-only, this crate
is `std` through tokio and eyre, and the answer turned out to be that its
hydrate path owns its own pages and depends on nothing here. A child crate
was built to bridge the two and then reverted.

So this is wanted for its own sake: 21 of 26 files already touch no `fs`,
`io` or `tokio`, the `std` half of the crate is five files, and the
separation is worth having whether or not anybody downstream asks for it.

The one real consumer left is `~/code/databucket-loader`, an unpublished crate with no
`.git` at all that `moe-pgo` depends on by path, which means a fresh clone of
`moe-pgo` cannot build. It sits directly on `data_bucket` and does mmap
attachment, digest identity and a lease registry over cluster bundles.

## Corrections to the record

Three things were asserted earlier in this line of work and are wrong. They are
recorded here so nobody rebuilds on them.

**`databucket-loader` is not a WorkTable reimplementation, and retiring it into
WorkTable is not the goal.** It was described as "1,548 lines reimplementing a
layer WorkTable already owns". It does not depend on WorkTable at all. It
provides read-only mmap attachment, an `AttachPolicy` that refuses
group/world-readable files and `mlock`s the mapping, digest verification at
attach, and a registry keyed by content digest whose unload semantics are the
opposite of WorkTable's: dropping the registry entry keeps existing leases
valid. WorkTable's `unload_gracefully` fails while a lease remains. That claim
is also baked into the doc comment of
`~/code/WorkTable/tests/generation_swap_requirement.rs`, which should be
corrected by whoever owns that repo.

**The zero-copy argument against hydrating into `-Vec` was wrong.** Every
consumer in `moe-pgo` calls `ClusterSpace::to_vec()`, at all four call sites,
and `for_each_chunk` has no users outside the crate that defines it. The bytes
are already copied into an owned `Vec<u8>` on every access, and several paths
then brotli-decompress into a second one. Real packs are 49 KB to 2.8 MB, 6.5 MB
for all five. Hydrate-at-load, `Vec` in memory, serialize-at-flush is the
correct model for this workload.

**`tokio` is not the wall it looks like.** See below.

## Measured: `tokio::fs` costs 10.8x on this crate's own access pattern

`page::util` reads a page as seek, then `read_exact` the header, then
`read_exact` the body. `tokio::fs` has no true async file I/O on POSIX, so each
of those is dispatched to a blocking thread pool.

```
4096 pages of 16384 B, 2000 random reads per rep, 7 reps, warm page cache

  tokio::fs            median 31.68 ms    15.84 us/page
  std::fs (blocking)   median  2.94 ms     1.47 us/page
  null (tokio again)   median 31.51 ms    15.76 us/page

  tokio / std   10.79x
  tokio / null   1.01x   <- the floor
```

**The null arm is the floor and it is the reason to believe the rest.** It runs
the tokio path a second time under a different name. Identical code separates by
1%, so a 979% gap is the workload and not the harness. About 14.4 us of overhead
per page, roughly 4.8 us per call across the seek and two reads, which is what a
thread-pool handoff costs.

The harness is at
`/private/tmp/claude-501/-Users-revenge-code/ba993238-5b8d-4d6e-b1bf-1a306b65af36/scratchpad/iobench`.
It is scratch and will not survive; rebuild it in-repo as a bench if the number
needs to be defended. It is bounded on purpose: fixed page count, fixed reps, and
a 120 second wall-clock abort.

Two things make the comparison fair here. The page cache is warm for both arms,
which is the normal case for repeated page reads. And these functions take
`&mut File`, so they are inherently sequential and cannot use the concurrency the
thread pool would otherwise buy.

## What actually blocks `no_std`

The layout is already most of the way there. **21 of 26 files are pure format**;
only 5 touch `fs`, `io` or `tokio`.

| Blocker | Real size | Verdict |
|---|---|---|
| `tokio` | 13 real references, 8 more test-only | **Small.** One shape |
| `std::fs` / `std::io` | 29 uses, same 5 files | Falls out with tokio |
| `eyre` | 69 uses | Medium, shallow |
| `WorkTablesIndex` | 6 files | **Feature graph, not code** |

**tokio.** Usage is only `tokio::fs::` and `tokio::io::` with
`AsyncReadExt`/`AsyncSeekExt`/`AsyncWriteExt`. No runtime, no spawn, no
channels, nothing for a work-stealing queue like `ps-st3` to replace. All 19
async fns are in `src/page/util.rs`. Every real site takes
`file: &mut tokio::fs::File` as a **concrete type**. The manifest says
`features = ["full"]`, which pulls the whole runtime for nine `fs` calls and
seven `io` calls, and should be narrowed regardless of this work.

**eyre.** Wide but shallow, and mostly in the half that stays behind the std
gate. The format files are already nearly clean: `header.rs`, `sized.rs`,
`persistable.rs`, `table_of_contents_page.rs` and `rkyv_data.rs` have **zero**
eyre references. Only `data.rs` has four. `derive_more` is already a dependency
and works `no_std`, so the error type has somewhere to go. The catch is that
`eyre::Result` is in the public signature of the format half, so this is a
breaking change for WorkTable.

**WorkTablesIndex, which was called the real blocker and is smaller than that.**
`parking_lot` appears **only** in `src/concurrent/*` (`operation.rs`, `set.rs`,
`ref.rs`, `multimap.rs`, `map.rs`). The three things `data_bucket` actually
imports are `cdc::change::ChangeEvent`, `core::pair::Pair` and
`core::multipair::MultiPair`, and all three have **zero** `parking_lot`
references. The problem is purely the feature graph: `cdc = ["concurrent"]` and
`multimap = ["concurrent", ...]`, and `concurrent = ["dep:parking_lot"]`, so
asking for the types drags in the map. In `util/sized.rs` the entire dependency
is four trait impls on foreign types (`SizeMeasurable` and
`VariableSizeMeasurable` for `Pair` and `MultiPair`), which is trivially
gateable.

The other dependencies are fine behind `default-features = false`: `rkyv`,
`uuid`, `ordered-float`, `psc-nanoid`, `derive_more`.

## The shape being proposed

**Unverified.** This is the design to argue with, not a decision already taken.

1. **No child crate, and `WorkTable-vec` does not depend on this one.**
   That was tried and reverted. `WorkTable-vec` is `no_std` and alloc-only,
   `data_bucket` is `std` through tokio and eyre, and carving out a second
   crate here to bridge that was answering a question nobody asked. The
   hydrate path lives in `WorkTable-vec` as its own functions over its own
   pages, and owes this crate nothing. If it ever earns a crate of its own
   it is called `wt-hydrate` and it lives there, not here.
2. What remains for this repo is its own `no_std` story, wanted for its own
   sake rather than for a consumer: the format half already has no `fs`,
   `io` or `tokio` in 21 of 26 files.
3. The page functions become generic over a read/seek/write bound instead of
   naming `tokio::fs::File`. The bound compiles away to nothing either way. The
   cheapest option is a local trait of the three methods this crate actually
   calls, which adds no dependency and commits the format to nobody's
   ecosystem. `acid-io` and `core2` are off-the-shelf alternatives if a local
   trait turns out not to be enough.

**This is a container format.** It cares about `no_std` and about nothing
else. No target belongs in its manifest, its comments or its features, and a
dependency is gated for what it drags in.

### The caveat that shapes the work

You cannot simply swap blocking `std::fs` in and take the 10.8x. WorkTable's
persistence engine is async on tokio, and blocking reads on a runtime worker
stall the executor. The correct shape is one `spawn_blocking` around a **batch**
of page operations rather than per call. That captures most of the win and keeps
the async boundary honest. `persist_pages_batch` already has that shape on the
write side; the read side does not.

## Order of work, cheapest first

Each step names what tells you it worked. Stop after any of them; they are
useful in this order and none requires the next.

1. **Narrow the tokio feature list** from `full` to `fs` plus `io-util`.
   Check: the workspace builds and the tests pass.
2. **Decide whether a `-Vec` hydrate needs index pages at all.** If it needs
   only data pages and the table of contents, the whole `parking_lot` question
   stays in the parent crate and steps 3 and 6 get much smaller. **This is the
   cheapest question with the largest effect on everything after it, so answer
   it before doing anything else structural.**
3. **Split the feature graph in WorkTablesIndex** so `ChangeEvent`, `Pair` and
   `MultiPair` are reachable without `concurrent`. Check: `data_bucket` compiles
   with `parking_lot` absent from `cargo tree`.
4. **Make the page functions generic** over a read/seek/write bound. Check: the
   10.8x is reproducible through the new bound with a blocking implementation,
   and the null arm still says 1.01x.
5. **Batch the read path** behind one `spawn_blocking`. Check: WorkTable's
   suite passes and the executor is not stalled under load.
6. **Gate the I/O layer behind a `std` feature** so the format half can build
   without it. Check: `--no-default-features` compiles with no `std` in
   `cargo tree`.
7. **Replace `eyre` in the format half.** Check: it compiles under `no_std`.

## Do not

- **Do not retire `databucket-loader` into WorkTable or into `-Vec`.** It is not
  duplicated work. See the corrections above.
- **Do not treat this as a way to make `moe-pgo` build from a fresh clone.**
  That needs `databucket-loader` published or vendored into the workspace, and
  it is a separate decision.
- **Do not change a page layout while you are here.** This repo's own rule: a
  layout change is a data-format change, `PAGE_SIZE`, `INNER_PAGE_SIZE`,
  `GENERAL_HEADER_SIZE` and every `#[derive(Archive)]` shape are read back out
  of files an earlier build wrote, and it needs a `DATA_VERSION` bump in the
  same change.
- **Do not bump the version unless you mean to ship.** A version bump merged to
  `master` publishes to crates.io with no staging step.
- **Do not uncomment the `indexset` path or `wt-indexset` lines** in
  `Cargo.toml`. They are local-work switches and committing one publishes a
  crate that does not build for anyone else.

## A defect found on the way, unrelated to this work

`AttachPolicy::sensitive()` in `~/code/databucket-loader` sets
`lock_memory: true` and `mlock`s the mapping so weights cannot reach swap, with
a comment calling it fail-closed because continuing silently "would violate the
caller's confidentiality policy". Then `ClusterSpace::to_vec()` copies those
bytes into ordinary unlocked heap, on every path, because that is the only
accessor anyone uses. The guarantee is defeated one call later. Either the copy
lands in locked memory too, or the `mlock` is ceremony and should go.

## Repos and paths

| | |
|---|---|
| this crate | `/Users/revenge/code/DataBucket`, branch `chore/caret-worktablesindex` |
| the index | `/Users/revenge/code/WorkTablesIndex` |
| the Vec tables | `/Users/revenge/code/WorkTable-vec`, branch `feat/atomic-table-vocabulary` |
| the loader | `/Users/revenge/code/databucket-loader` (no `.git`) |
| the consumer | `/Users/revenge/code/moe-pgo`, branch `master` |
| WorkTable | `/Users/revenge/code/WorkTable`, another agent's live checkout |

`~/code/WorkTable` is worked in by another agent. Do not build in it or edit it
without asking.

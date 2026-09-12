# db-qa-upstash

End-to-end latency, throughput, integrity and commit-protocol gate for a
DataBucket page store on Upstash Redis. This is a binary QA crate, not a Rust
test suite.

The crate depends on the repository's `data_bucket` package with its default
`no_std` dependency graph and takes `PAGE_SIZE` from that public API. Network,
TLS, Fly orchestration and reporting remain in this QA binary and never enter
the library graph.

The manual `Remote store QA` workflow follows the API service pipeline. It
builds the `x86_64-unknown-linux-musl` executable directly on an Ubicloud
runner, then Buildah creates a scratch image containing only that executable.
The running image has no shell, libc, language runtime or package manager.

Dispatch the workflow with an existing disposable Fly application. The binary
creates a 72-hour Upstash database from inside the Fly Machine, uses only its
own key prefix, validates reads and atomic head advances, deletes its keys, and
emits a single `DB_QA_UPSTASH_JSON=` record to Fly logs.

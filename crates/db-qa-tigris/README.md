# db-qa-tigris

This executable measures the remote object-store operations needed by a
DataBucket page store. It uses the same S3-compatible protocol for Tigris,
Cloudflare R2 and Bunny Storage so results are directly comparable.

The executable is an external end-to-end driver, not a Rust test. It writes
under a unique `db-qa/` prefix, verifies every read, checks conditional object
writes, deletes its objects and emits one `DB_QA_S3_JSON=` record. Its page
size comes from `data_bucket::PAGE_SIZE`.

The container is one static Rust binary in a scratch image. HTTP and S3 code
remain outside the `data_bucket` library, whose default dependency graph must
continue to compile without `std`.

Required environment variables are `DB_QA_PROVIDER`, `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. The endpoint, region and bucket may use the generic
`S3_ENDPOINT`, `S3_REGION` and `S3_BUCKET` names or Fly's Tigris names:
`AWS_ENDPOINT_URL_S3`, `AWS_REGION` and `BUCKET_NAME`. Set
`S3_URL_STYLE=virtual` only for providers that require virtual-hosted bucket
URLs. The default is path-style URLs.

For Cloudflare R2, keep the parent token local and create a short-lived,
prefix-scoped credential for the Fly runner:

```text
CLOUDFLARE_ACCOUNT_ID=<account-id> \
S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com \
S3_BUCKET=<bucket> \
FLY_APP_NAME=<disposable-app> \
cargo run --manifest-path crates/db-qa-tigris/Cargo.toml \
  --bin db-qa-r2-session
```

`db-qa-r2-session` signs a one-hour child credential locally, restricts it to
the `db-qa/` prefix and the four object operations used by this harness, and
imports it into Fly without printing it. The parent secret never leaves the
local process. It reads the parent Access Key ID and Secret Access Key from
hidden prompts. Automation may instead provide the standard `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY` environment variables. Set
`R2_SESSION_TTL_SECONDS` to shorten the lifetime.

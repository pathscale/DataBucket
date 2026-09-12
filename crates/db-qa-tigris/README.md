# db-qa-tigris

This executable measures the remote object-store operations needed by a
DataBucket page store. It uses the same S3-compatible protocol for Tigris,
Cloudflare R2 and Bunny Storage so results are directly comparable.

The executable is an external end-to-end driver, not a Rust test. It writes
under a unique `db-qa/` prefix, verifies every read, checks conditional object
writes, deletes its objects and emits one `DB_QA_S3_JSON=` record. Its page
size comes from `data_bucket::PAGE_SIZE`.

The manual `Remote store QA` workflow builds the musl executable directly on
an Ubicloud runner. Buildah then creates a scratch image containing only that
executable and pushes it to the selected disposable Fly application. Rust is
never installed or run inside an image. The benchmark's parallel request
machinery stays in this crate. DataBucket's production S3 adapter is optional
behind `s3-support`, while its default dependency graph continues to compile
without `std`.

Required environment variables are `DB_QA_PROVIDER`, `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. The endpoint, region and bucket may use the generic
`S3_ENDPOINT`, `S3_REGION` and `S3_BUCKET` names or Fly's Tigris names:
`AWS_ENDPOINT_URL_S3`, `AWS_REGION` and `BUCKET_NAME`. Set
`S3_URL_STYLE=virtual` only for providers that require virtual-hosted bucket
URLs. The default is path-style URLs.

For Cloudflare R2, keep the parent token local and create a short-lived,
prefix-scoped credential for the Fly runner:

```text
FLY_APP_NAME=<disposable-app> \
doppler run --project api-support-cafe --config dev -- \
  cargo run --manifest-path crates/db-qa-tigris/Cargo.toml \
    --bin db-qa-r2-session
```

`db-qa-r2-session` signs a one-hour child credential locally, restricts its
object read/write scope to the `db-qa/` prefix, and imports it into Fly without
printing it. The parent secret never leaves the
local process. It reads the parent Access Key ID and Secret Access Key from
hidden prompts. Automation may instead provide the standard `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY` environment variables, or the namespaced
`CAFE__R2__*` variables used by the `api-support-cafe` development config. Set
`R2_SESSION_TTL_SECONDS` to shorten the lifetime.

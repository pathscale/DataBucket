# DataBucket

Page framing, row directories and index storage for WorkTable.

## Version 3 cutover

DataBucket 0.7 writes page format 3. Older page formats are rejected with a
version error; opening them does not convert or remove them. Deployments that
can regenerate their data should explicitly recreate the store. Retained data
requires an application-specific conversion with the old reader.

For a page of P bytes, the general header occupies the first 28 bytes. Data
pages contain row bytes followed by a live-row directory at the page tail.
Each directory entry is a little-endian `(u32 offset, u32 length)` pair; its
offset is relative to the payload. A CRC-32 is stored at P-8 and the entry
count at P-4. The checksum covers the entire payload except its own word,
including padding, directory and count. Header identity and version are
validated separately. Index and metadata pages retain their existing body
layouts, with the new page-version marker.

`data_page_row_capacity` computes a row allocation budget that reserves the
maximum directory space for the minimum archived row size. `DataPage::encode`
and `decode` validate directory extents and integrity. Updates, deletion and
relocation must maintain `DataPage::rows`; raw row bytes alone are not a
complete v3 persisted data page. The file-level `update_at` accepts a readable
and writable file, validates the existing live row and updates the checksum.

The table schema version and crate package version are separate from the
page-format version. WorkTable's Vec snapshot files use a different container
and cannot be treated as ordinary DataBucket space files.

## Command line tools

Create a sample file containing 2,500 records. An existing file is refused:

```sh
cargo run -p create-data-file -- --filename sample.wt.data --count 2500
```

Inspect page identities and live row extents without consulting an index:

```sh
cargo run -p dump-data-file -- --filename sample.wt.data
cargo run -p dump-data-file -- --filename sample.wt.data --hex
```

`--count` is a record count. The dumper reports each row's payload offset,
archive length and absolute file offset. `--hex` includes its archive bytes;
typed deserialization requires the owning application's row schema. For a
nondefault stride, supply `--page-size`; supported values are 512, 4096, 8192,
16384 and 32768 bytes. The tools validate data-page checksums and directories.

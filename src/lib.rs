extern crate core;

// The Persistable derive emits paths through the crate name, and this crate
// uses its own derive: alias ourselves so the generated code resolves here too.
extern crate self as data_bucket;

pub mod error;

/// A file this crate can read and write, without naming whose runtime owns it.
///
/// **The signatures used to say `async_fs::File`.** That is a concrete type
/// belonging to one runtime, so every caller inherited that runtime whether it
/// wanted it or not, and `no_std` was impossible while it was there. Nothing in
/// this crate ever needed a `File`: the whole surface is `seek`, `read`,
/// `read_exact` and `write_all`, which are trait methods. `sync_all` and
/// `metadata` appear only in tests.
///
/// **They then came from `futures-io`, and that was wrong for the stated
/// reason.** The claim above it was that `futures-io` is `no_std` once its
/// `std` feature comes off. It is not: *every one of its traits sits behind
/// that feature*, so turning it off leaves the crate exporting nothing at all.
/// They take `std::io::Error` and `IoSlice`, so there was nowhere else for them
/// to go.
///
/// That is easy to check the wrong way: the crate still *compiles* with the
/// feature off, so a probe that only builds it reports success, and it is the
/// exports that vanish. `AsyncFile` was therefore a `std` trait wearing a
/// portable name, and this crate linked `std` through it however carefully the
/// rest of it was written.
///
/// `nagoya::io` declares these traits over a portable error. Durability and
/// file-management methods belong to the storage engine, not page framing.
/// `+ Send` because this crate's futures cross a task boundary and the
/// provided methods on those traits capture `&mut self`, so the future is only
/// `Send` if the file is.
pub trait AsyncFile: AsyncRead + AsyncWrite {}

impl<T: ?Sized> AsyncFile for T where T: AsyncRead + AsyncWrite {}

/// Read/seek capabilities needed by page decoders; no write permission or
/// durability implementation is required.
pub trait AsyncRead: nagoya::io::Read + nagoya::io::Seek + Send {}
impl<T: ?Sized> AsyncRead for T where T: nagoya::io::Read + nagoya::io::Seek + Send {}

/// Write/seek capabilities needed by page encoders.
pub trait AsyncWrite: nagoya::io::Write + nagoya::io::Seek + Send {}
impl<T: ?Sized> AsyncWrite for T where T: nagoya::io::Write + nagoya::io::Seek + Send {}

pub mod link;
pub mod page;
pub mod persistence;
pub mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::{SizeMeasure, VariableSizeMeasure};
pub use page::{
    get_index_page_size_from_data_length, map_data_pages_to_general, parse_data_page,
    parse_data_pages_batch, parse_general_header_by_index, parse_page, parse_pages_batch,
    persist_page, persist_pages_batch, seek_by_link, seek_to_page_start, update_at, DataPage,
    GeneralHeader, GeneralPage, IndexPage, IndexPageUtility, IndexValue, Interval,
    PageOverflowError, PageType, SpaceInfoPage, TableOfContentsOverflowError, TableOfContentsPage,
    UnsizedIndexPage, UnsizedIndexPageUtility, DATA_VERSION, DEFAULT_PAGE_STRIDE,
    EMPTY_TABLE_OF_CONTENTS_PAGE_SIZE, GENERAL_HEADER_SIZE, INNER_PAGE_SIZE, PAGE_SIZE,
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use space::Id as SpaceId;
pub use util::access_archived;
pub use util::{
    align, align8, align_to, align_vec, Persistable, SizeMeasurable, VariableSizeMeasurable,
};

//! What this crate can refuse on.
//!
//! # Why this exists rather than `eyre`
//!
//! `eyre::Report` is a `std` type, and it was in the return type of every
//! fallible function here, so the whole crate reached `std` through its own
//! signatures. Nothing else about page framing needs an operating system: the
//! layout is bytes, the checks are arithmetic. This is the type that lets the
//! rest of the crate say so.
//!
//! It is also more useful than a formatted string. A caller that wants to
//! distinguish "this page is full" from "these bytes are damaged" could not,
//! because both arrived as a `Report` carrying prose.
//!
//! Consumers keep working: `Error` implements `core::error::Error`, so `?`
//! into an `eyre::Result` converts exactly as it did before.

use core::fmt::{Display, Formatter, Result as FmtResult};

use crate::page::PageId;

/// The result of anything in this crate that can fail.
pub type Result<T> = core::result::Result<T, Error>;

/// A refusal, with the numbers that justify it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// A write's length does not match the link it was given.
    ///
    /// A `Link` names an exact byte range, so a write of another size is not
    /// a resize: it is a write that would land on the neighbouring row.
    LinkLengthMismatch {
        /// The length the link reserves.
        expected: u32,
        /// The length the caller supplied.
        found: usize,
    },
    /// A link's range runs past the end of the page it points into.
    ///
    /// Summed in `u64`, because `offset + length` in `u32` can wrap past 4 GiB
    /// and slip under the bound.
    LinkOutOfBounds {
        /// Where the link starts.
        offset: u32,
        /// How far it runs.
        length: u32,
        /// How much the page holds.
        capacity: usize,
    },
    /// A page's contents do not fit the page.
    ///
    /// Raised where the write is prepared rather than where it lands, so an
    /// over-budget page fails in its own persist instead of quietly writing
    /// into its neighbour.
    PageOverflow {
        /// The page whose write is over budget.
        page: PageId,
        /// Bytes the write needs.
        needed: usize,
        /// Bytes available.
        capacity: usize,
    },
    /// Bytes that should have been a structure were not one.
    ///
    /// The label says which structure, because "corrupt" on its own does not
    /// tell an operator which part of a file to distrust.
    Corrupt {
        /// What failed to parse.
        what: &'static str,
    },
    /// A value would not archive.
    ///
    /// rkyv reports this when its allocator refuses, which on this path means
    /// the process is already out of memory.
    Encode,
    /// The file would not answer.
    ///
    /// **This used to hold a raw OS code**, taken from `std::io::Error`, with
    /// the note that the code is what an operator acts on. That was right while
    /// the crate opened files itself. It no longer does: the file arrives as
    /// `nagoya::io::File`, whose error is deliberately coarse and carries no OS
    /// code at all, because a caller either retries, gives up, or creates what
    /// was missing.
    ///
    /// So the kind is carried instead. Mapping it back to `code: None` would
    /// have compiled and made every failure identical, which is worse than
    /// losing the number: "no such file" and "permission denied" are the two an
    /// operator most needs told apart.
    Io {
        /// What kind of failure the file reported.
        kind: nagoya::io::ErrorKind,
    },
    /// A change-data event arrived that this page cannot apply.
    ///
    /// `SplitNode`, `CreateNode` and `RemoveNode` change which pages exist,
    /// which is the caller's business rather than one page's.
    UnapplicableEvent,
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::LinkLengthMismatch { expected, found } => write!(
                formatter,
                "a {found} byte write does not match its {expected} byte link"
            ),
            Self::LinkOutOfBounds {
                offset,
                length,
                capacity,
            } => write!(
                formatter,
                "a link at {offset} running {length} bytes leaves a {capacity} byte page"
            ),
            Self::PageOverflow {
                page,
                needed,
                capacity,
            } => write!(
                formatter,
                "page {page:?} needs {needed} bytes of a {capacity} byte page"
            ),
            Self::Corrupt { what } => write!(formatter, "torn or corrupt {what}"),
            Self::Encode => write!(formatter, "a value would not archive"),
            Self::Io { kind } => write!(formatter, "the file failed: {kind:?}"),
            Self::UnapplicableEvent => write!(
                formatter,
                "events of `SplitNode`, `CreateNode` or `RemoveNode` cannot be applied to a page"
            ),
        }
    }
}

impl core::error::Error for Error {}

// The note here used to read "not gated yet: the crate still reaches the file
// system directly. When that moves behind a trait this impl goes with it."
// That is what happened. The file is `nagoya::io::File` now, so this converts
// from its error and `std::io::Error` no longer appears in this crate at all,
// which is what the module documentation above claims and could not deliver on
// its own.
impl From<nagoya::io::Error> for Error {
    fn from(error: nagoya::io::Error) -> Self {
        Self::Io { kind: error.kind() }
    }
}

impl From<rkyv::rancor::Error> for Error {
    fn from(_: rkyv::rancor::Error) -> Self {
        Self::Encode
    }
}

impl From<crate::page::PageOverflowError> for Error {
    fn from(error: crate::page::PageOverflowError) -> Self {
        Self::PageOverflow {
            page: error.page_id,
            needed: error.data_length,
            capacity: error.capacity,
        }
    }
}

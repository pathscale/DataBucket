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
    /// Held as the raw OS code rather than an `io::Error`, because that type
    /// is `std` and this one is the reason the crate stops needing it. The
    /// code is what an operator acts on; the message that came with it says
    /// nothing the code does not.
    Io {
        /// `raw_os_error`, when the failure came from the operating system.
        code: Option<i32>,
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
            Self::Io { code: Some(code) } => write!(formatter, "the file failed, os error {code}"),
            Self::Io { code: None } => write!(formatter, "the file failed"),
            Self::UnapplicableEvent => write!(
                formatter,
                "events of `SplitNode`, `CreateNode` or `RemoveNode` cannot be applied to a page"
            ),
        }
    }
}

impl core::error::Error for Error {}

// Not gated yet: the crate still reaches the file system directly. When that
// moves behind a trait this impl goes with it.
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            code: error.raw_os_error(),
        }
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

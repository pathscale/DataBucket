#![no_std]

use data_bucket::{SizeMeasurable, SizeMeasure};
use rkyv::Archive;

#[derive(Archive, SizeMeasure)]
pub enum Kind { Row, Index }

pub fn archived_size(kind: Kind) -> usize { kind.aligned_size() }

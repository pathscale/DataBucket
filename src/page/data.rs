use crate::error::{Error, Result};
use crate::Link;
use crate::Persistable;
use alloc::vec::Vec;

#[derive(Debug)]
pub struct DataPage<const DATA_LENGTH: usize> {
    pub length: u32,
    pub data: [u8; DATA_LENGTH],
    /// Live rows, ordered by their payload-relative offset.
    pub rows: Vec<RowSlot>,
}

/// One v3 directory entry. Both integers are little endian on disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowSlot {
    pub offset: u32,
    pub length: u32,
}

pub const ROW_SLOT_SIZE: usize = 8;
pub const DATA_TRAILER_SIZE: usize = 8;

/// Identity and exact live-row accounting decoded from one complete v3 page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataPageImageFacts {
    pub page_id: crate::page::PageId,
    pub space_id: crate::SpaceId,
    pub live_rows: u32,
    pub live_bytes: u32,
}

/// Validates a complete encoded v3 data page without knowing its row type.
pub fn inspect_data_page_image(bytes: &[u8]) -> crate::error::Result<DataPageImageFacts> {
    let header = inspect_page_image_header(bytes)?;
    if header.page_type != crate::PageType::Data {
        return Err(crate::error::Error::Corrupt {
            what: "data page type",
        });
    }
    let payload = &bytes[crate::GENERAL_HEADER_SIZE..];
    let rows = DataPage::<0>::directory(payload, header.data_length)?;
    let live_bytes = rows.iter().try_fold(0_u32, |total, row| {
        total
            .checked_add(row.length)
            .ok_or(crate::error::Error::Corrupt {
                what: "v3 live row bytes",
            })
    })?;
    let live_rows = u32::try_from(rows.len()).map_err(|_| crate::error::Error::Corrupt {
        what: "v3 row count",
    })?;
    Ok(DataPageImageFacts {
        page_id: header.page_id,
        space_id: header.space_id,
        live_rows,
        live_bytes,
    })
}

/// Validates and decodes the header shared by every complete page image.
pub fn inspect_page_image_header(bytes: &[u8]) -> crate::error::Result<crate::GeneralHeader> {
    if bytes.len() < crate::GENERAL_HEADER_SIZE + DATA_TRAILER_SIZE {
        return Err(crate::error::Error::Corrupt {
            what: "v3 data page image",
        });
    }
    let header_bytes = &bytes[..crate::GENERAL_HEADER_SIZE];
    let archived = rkyv::access::<
        <crate::GeneralHeader as rkyv::Archive>::Archived,
        rkyv::rancor::Error,
    >(header_bytes)
    .map_err(|_| crate::error::Error::Corrupt {
        what: "page header",
    })?;
    let header: crate::GeneralHeader = rkyv::deserialize::<_, rkyv::rancor::Error>(archived)
        .map_err(|_| crate::error::Error::Corrupt {
            what: "page header",
        })?;
    if header.data_version != crate::DATA_VERSION {
        return Err(crate::error::Error::UnsupportedVersion {
            found: header.data_version,
            expected: crate::DATA_VERSION,
        });
    }
    let payload = &bytes[crate::GENERAL_HEADER_SIZE..];
    if header.data_length as usize > payload.len() {
        return Err(crate::error::Error::Corrupt {
            what: "v3 row extent",
        });
    }
    Ok(header)
}

/// Row-byte capacity that leaves room for every possible directory entry.
/// `minimum_row_size` is the size of the archived row wrapper. Variable-size
/// archives can only be larger. The reservation is independent of row contents
/// so mutations never discover a full directory after publishing an index.
pub const fn data_page_row_capacity(stride: usize, minimum_row_size: usize) -> usize {
    assert!(
        minimum_row_size > 0,
        "a row must have a nonzero archived size"
    );
    let payload = stride.saturating_sub(crate::GENERAL_HEADER_SIZE + DATA_TRAILER_SIZE);
    let slots = payload / (minimum_row_size + ROW_SLOT_SIZE);
    let capacity = payload - slots * ROW_SLOT_SIZE;
    let next_row = slots.saturating_add(1).saturating_mul(minimum_row_size);
    if capacity < next_row {
        capacity
    } else {
        next_row.saturating_sub(1)
    }
}

impl<const DATA_LENGTH: usize> DataPage<DATA_LENGTH> {
    pub fn new() -> Self {
        Self {
            length: 0,
            data: [0; DATA_LENGTH],
            rows: Vec::new(),
        }
    }

    /// Remove only the exact live row named by this mutation. A delayed
    /// delete must never erase a replacement with a different extent.
    pub fn remove_at(&mut self, link: Link) {
        self.rows
            .retain(|slot| slot.offset != link.offset || slot.length != link.length);
    }

    pub fn decode(bytes: &[u8], length: u32) -> Result<Self> {
        if length as usize > DATA_LENGTH {
            return Err(Error::Corrupt {
                what: "v3 row extent",
            });
        }
        let rows = Self::directory(bytes, length)?;
        let mut page = Self::new();
        page.length = length;
        page.data[..length as usize].copy_from_slice(&bytes[..length as usize]);
        page.rows = rows;
        Ok(page)
    }

    pub(crate) fn directory(bytes: &[u8], length: u32) -> Result<Vec<RowSlot>> {
        let corrupt = || Error::Corrupt {
            what: "v3 data page directory",
        };
        if bytes.len() < DATA_TRAILER_SIZE {
            return Err(corrupt());
        }
        let word = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let tail = bytes.len() - DATA_TRAILER_SIZE;
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[..tail]);
        crc.update(&bytes[tail + 4..]);
        if crc.finalize() != word(tail) {
            return Err(Error::Corrupt {
                what: "v3 data page checksum",
            });
        }
        let count = word(tail + 4) as usize;
        if count > tail / ROW_SLOT_SIZE {
            return Err(corrupt());
        }
        let directory = tail - count * ROW_SLOT_SIZE;
        if length as usize > directory {
            return Err(corrupt());
        }
        let mut rows = Vec::with_capacity(count);
        let mut previous_end = 0u64;
        for at in (directory..tail).step_by(ROW_SLOT_SIZE) {
            let slot = RowSlot {
                offset: word(at),
                length: word(at + 4),
            };
            let end = u64::from(slot.offset) + u64::from(slot.length);
            if slot.length == 0 || u64::from(slot.offset) < previous_end || end > u64::from(length)
            {
                return Err(corrupt());
            }
            previous_end = end;
            rows.push(slot);
        }
        Ok(rows)
    }

    pub fn encode(&self, capacity: usize) -> Result<Vec<u8>> {
        let directory_bytes = self
            .rows
            .len()
            .checked_mul(ROW_SLOT_SIZE)
            .and_then(|n| n.checked_add(DATA_TRAILER_SIZE))
            .ok_or(Error::Corrupt {
                what: "v3 directory size",
            })?;
        let needed = (self.length as usize)
            .checked_add(directory_bytes)
            .ok_or(Error::Corrupt {
                what: "v3 page size",
            })?;
        if needed > capacity || self.length as usize > DATA_LENGTH {
            return Err(Error::PageOverflow {
                page: 0.into(),
                needed,
                capacity,
            });
        }
        let mut bytes = vec![0; capacity];
        bytes[..self.length as usize].copy_from_slice(&self.data[..self.length as usize]);
        let tail = capacity - DATA_TRAILER_SIZE;
        let start = capacity - directory_bytes;
        let mut previous_end = 0u64;
        for (slot, at) in self.rows.iter().zip((start..tail).step_by(ROW_SLOT_SIZE)) {
            let end = u64::from(slot.offset) + u64::from(slot.length);
            if slot.length == 0
                || u64::from(slot.offset) < previous_end
                || end > u64::from(self.length)
            {
                return Err(Error::Corrupt {
                    what: "v3 data page directory",
                });
            }
            previous_end = end;
            bytes[at..at + 4].copy_from_slice(&slot.offset.to_le_bytes());
            bytes[at + 4..at + 8].copy_from_slice(&slot.length.to_le_bytes());
        }
        bytes[tail + 4..].copy_from_slice(&(self.rows.len() as u32).to_le_bytes());
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[..tail]);
        crc.update(&bytes[tail + 4..]);
        bytes[tail..tail + 4].copy_from_slice(&crc.finalize().to_le_bytes());
        Ok(bytes)
    }

    pub fn update_at(&mut self, link: Link, new_data: &[u8]) -> Result<()> {
        if new_data.len() as u32 != link.length {
            return Err(Error::LinkLengthMismatch {
                expected: link.length,
                found: new_data.len(),
            });
        }

        // Sum in usize: `offset + length` in u32 can wrap past 4 GiB and
        // slip under the bound with a range that is actually out of page.
        let start = link.offset as usize;
        let end = link.offset as usize + link.length as usize;
        if end > DATA_LENGTH {
            return Err(Error::LinkOutOfBounds {
                offset: link.offset,
                length: link.length,
                capacity: DATA_LENGTH,
            });
        }

        self.data[start..end].copy_from_slice(new_data);

        // Reuse can split a former row's extent. Its old directory entry
        // must disappear before any newly overlapping row is published.
        self.rows.retain(|slot| {
            let slot_end = u64::from(slot.offset) + u64::from(slot.length);
            slot_end <= start as u64 || u64::from(slot.offset) >= end as u64
        });
        if link.length > 0 {
            let position = self.rows.partition_point(|slot| slot.offset < link.offset);
            self.rows.insert(
                position,
                RowSlot {
                    offset: link.offset,
                    length: link.length,
                },
            );
        }

        self.length = self.length.max(end as u32);
        Ok(())
    }

    pub fn get_at(&self, link: Link) -> Result<&[u8]> {
        // Sum in usize, see `update_at`.
        let start = link.offset as usize;
        let end = link.offset as usize + link.length as usize;
        if end > DATA_LENGTH {
            return Err(Error::LinkOutOfBounds {
                offset: link.offset,
                length: link.length,
                capacity: DATA_LENGTH,
            });
        }

        Ok(&self.data[start..end])
    }
}

impl<const DATA_LENGTH: usize> Persistable for DataPage<DATA_LENGTH> {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        &self.data[..self.length as usize]
    }

    fn from_bytes(bytes: &[u8], _version: u32) -> Self {
        let mut data = [0; DATA_LENGTH];
        data.copy_from_slice(bytes);
        Self {
            length: bytes.len() as u32,
            data,
            rows: Vec::new(),
        }
    }

    fn page_bytes(&self, capacity: usize) -> Result<impl AsRef<[u8]> + Send> {
        self.encode(capacity)
    }

    fn page_data_length(&self, _encoded_length: usize) -> usize {
        self.length as usize
    }
}

impl<const DATA_LENGTH: usize> Default for DataPage<DATA_LENGTH> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(offset: u32, length: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length,
        }
    }

    #[test]
    fn v3_directory_roundtrips_live_rows_and_tracks_reuse() {
        let mut page = DataPage::<128>::new();
        page.update_at(link(0, 16), &[1; 16]).unwrap();
        page.update_at(link(32, 8), &[2; 8]).unwrap();
        page.remove_at(link(0, 16));
        page.update_at(link(0, 8), &[3; 8]).unwrap();
        page.update_at(link(8, 8), &[4; 8]).unwrap();
        let bytes = page.encode(160).unwrap();
        assert_eq!(u32::from_le_bytes(bytes[156..].try_into().unwrap()), 3);
        let parsed = DataPage::<128>::decode(&bytes, page.length).unwrap();
        let values: Vec<_> = parsed
            .rows
            .iter()
            .map(|slot| {
                parsed
                    .get_at(link(slot.offset, slot.length))
                    .unwrap()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![vec![3; 8], vec![4; 8], vec![2; 8]]);
    }

    #[test]
    fn v3_checksum_covers_rows_directory_count_and_padding() {
        let mut page = DataPage::<128>::new();
        page.update_at(link(0, 16), &[1; 16]).unwrap();
        let original = page.encode(160).unwrap();
        for byte in 0..original.len() {
            let mut damaged = original.clone();
            damaged[byte] ^= 1;
            assert!(
                DataPage::<128>::decode(&damaged, page.length).is_err(),
                "byte {byte}"
            );
        }
        for length in 0..original.len() {
            assert!(DataPage::<128>::decode(&original[..length], page.length).is_err());
        }
    }

    #[test]
    fn directory_reservation_fits_every_minimum_sized_row() {
        for stride in [512, 4096, 8192, 16384, 32768] {
            for minimum in [1, 8, 16, 24, 32, 48, 128, 8192] {
                let capacity = data_page_row_capacity(stride, minimum);
                let slots = capacity / minimum;
                assert!(
                    capacity
                        + slots * ROW_SLOT_SIZE
                        + DATA_TRAILER_SIZE
                        + crate::GENERAL_HEADER_SIZE
                        <= stride
                );
            }
        }
    }

    #[test]
    fn directory_cannot_overlap_row_bytes() {
        let mut page = DataPage::<64>::new();
        page.update_at(link(0, 64), &[1; 64]).unwrap();
        assert!(page.encode(64).is_err());
        assert!(page.encode(79).is_err());
        assert!(page.encode(80).is_ok());
    }

    #[test]
    fn malformed_directory_is_rejected_even_with_recomputed_checksum() {
        let mut page = DataPage::<64>::new();
        page.update_at(link(0, 16), &[1; 16]).unwrap();
        let mut bytes = page.encode(96).unwrap();
        // The entry is at 80. Make its length run beyond the row extent.
        bytes[84..88].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[..88]);
        crc.update(&bytes[92..]);
        bytes[88..92].copy_from_slice(&crc.finalize().to_le_bytes());
        assert!(DataPage::<64>::decode(&bytes, page.length).is_err());
    }

    #[test]
    fn test_update_at_success() {
        let mut data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 5,
            length: 3,
        };

        data.update_at(link, &[1, 2, 3]).unwrap();
        assert_eq!(data.get_at(link).unwrap(), &[1, 2, 3]);
        assert_eq!(data.length, 8);
    }

    #[test]
    fn test_update_at_wrong_length() {
        let mut data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 5,
            length: 3,
        };

        let err = data.update_at(link, &[1, 2]).unwrap_err();
        assert_eq!(
            err,
            Error::LinkLengthMismatch {
                expected: 3,
                found: 2
            }
        );
    }

    #[test]
    fn test_update_at_out_of_bounds() {
        let mut data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 98,
            length: 3,
        };

        let err = data.update_at(link, &[1, 2, 3]).unwrap_err();
        assert_eq!(
            err,
            Error::LinkOutOfBounds {
                offset: 98,
                length: 3,
                capacity: 100
            }
        );
    }

    #[test]
    fn test_update_at_offset_plus_length_wrapping_u32() {
        let mut data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        // In u32, offset + length wraps to 5 and used to pass the bounds
        // check, panicking on the slice instead of returning an error.
        let link = Link {
            page_id: 1.into(),
            offset: u32::MAX - 2,
            length: 8,
        };

        let err = data.update_at(link, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap_err();
        assert!(matches!(err, Error::LinkOutOfBounds { .. }));
    }

    #[test]
    fn test_get_at_offset_plus_length_wrapping_u32() {
        let data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: u32::MAX - 2,
            length: 8,
        };

        let err = data.get_at(link).unwrap_err();
        assert!(matches!(err, Error::LinkOutOfBounds { .. }));
    }

    #[test]
    fn test_get_at_out_of_bounds() {
        let data = DataPage {
            rows: Vec::new(),
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 98,
            length: 3,
        };

        let err = data.get_at(link).unwrap_err();
        assert_eq!(
            err,
            Error::LinkOutOfBounds {
                offset: 98,
                length: 3,
                capacity: 100
            }
        );
    }
}

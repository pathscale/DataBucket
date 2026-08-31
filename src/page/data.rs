use crate::Link;
use crate::Persistable;
use eyre::{eyre, Result};

#[derive(Debug)]
pub struct DataPage<const DATA_LENGTH: usize> {
    pub length: u32,
    pub data: [u8; DATA_LENGTH],
}

impl<const DATA_LENGTH: usize> DataPage<DATA_LENGTH> {
    pub fn update_at(&mut self, link: Link, new_data: &[u8]) -> Result<()> {
        if new_data.len() as u32 != link.length {
            return Err(eyre!(
                "New data length {} does not match link length {}",
                new_data.len(),
                link.length
            ));
        }

        // Sum in usize: `offset + length` in u32 can wrap past 4 GiB and
        // slip under the bound with a range that is actually out of page.
        let start = link.offset as usize;
        let end = link.offset as usize + link.length as usize;
        if end > DATA_LENGTH {
            return Err(eyre!(
                "Link range (offset: {}, length: {}) exceeds data bounds ({})",
                link.offset,
                link.length,
                DATA_LENGTH
            ));
        }

        self.data[start..end].copy_from_slice(new_data);

        self.length = self.length.max(end as u32);
        Ok(())
    }

    pub fn get_at(&self, link: Link) -> Result<&[u8]> {
        // Sum in usize, see `update_at`.
        let start = link.offset as usize;
        let end = link.offset as usize + link.length as usize;
        if end > DATA_LENGTH {
            return Err(eyre!(
                "Link range (offset: {}, length: {}) exceeds data bounds ({})",
                link.offset,
                link.length,
                DATA_LENGTH
            ));
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_at_success() {
        let mut data = DataPage {
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
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 5,
            length: 3,
        };

        let err = data.update_at(link, &[1, 2]).unwrap_err();
        assert!(err
            .to_string()
            .contains("New data length 2 does not match link length 3"));
    }

    #[test]
    fn test_update_at_out_of_bounds() {
        let mut data = DataPage {
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 98,
            length: 3,
        };

        let err = data.update_at(link, &[1, 2, 3]).unwrap_err();
        assert!(err
            .to_string()
            .contains("Link range (offset: 98, length: 3) exceeds data bounds (100)"));
    }

    #[test]
    fn test_update_at_offset_plus_length_wrapping_u32() {
        let mut data = DataPage {
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
        assert!(err.to_string().contains("exceeds data bounds"));
    }

    #[test]
    fn test_get_at_offset_plus_length_wrapping_u32() {
        let data = DataPage {
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: u32::MAX - 2,
            length: 8,
        };

        let err = data.get_at(link).unwrap_err();
        assert!(err.to_string().contains("exceeds data bounds"));
    }

    #[test]
    fn test_get_at_out_of_bounds() {
        let data = DataPage {
            length: 0,
            data: [0; 100],
        };

        let link = Link {
            page_id: 1.into(),
            offset: 98,
            length: 3,
        };

        let err = data.get_at(link).unwrap_err();
        assert!(err
            .to_string()
            .contains("Link range (offset: 98, length: 3) exceeds data bounds (100)"));
    }
}

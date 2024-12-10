use eyre::{eyre, Result};

use crate::Link;
use crate::Persistable;

#[derive(Debug)]
pub struct Data<const DATA_LENGTH: usize> {
    pub length: u32,
    pub data: [u8; DATA_LENGTH],
}

impl<const DATA_LENGTH: usize> Data<DATA_LENGTH> {
    pub fn update_at(&mut self, link: Link, new_data: &[u8]) -> Result<()> {
        if new_data.len() as u32 != link.length {
            return Err(eyre!(
                "New data length {} does not match link length {}",
                new_data.len(),
                link.length
            ));
        }

        if (link.offset + link.length) as usize > DATA_LENGTH {
            return Err(eyre!(
                "Link range (offset: {}, length: {}) exceeds data bounds ({})",
                link.offset,
                link.length,
                DATA_LENGTH
            ));
        }

        let start = link.offset as usize;
        let end = (link.offset + link.length) as usize;
        self.data[start..end].copy_from_slice(new_data);

        self.length = self.length.max(link.offset + link.length);
        Ok(())
    }

    pub fn get_at(&self, link: Link) -> Result<&[u8]> {
        if (link.offset + link.length) as usize > DATA_LENGTH {
            return Err(eyre!(
                "Link range (offset: {}, length: {}) exceeds data bounds ({})",
                link.offset,
                link.length,
                DATA_LENGTH
            ));
        }

        let start = link.offset as usize;
        let end = (link.offset + link.length) as usize;
        Ok(&self.data[start..end])
    }
}

impl<const DATA_LENGTH: usize> Persistable for Data<DATA_LENGTH> {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        &self.data[..self.length as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_at_success() {
        let mut data = Data {
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
        let mut data = Data {
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
        let mut data = Data {
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
    fn test_get_at_out_of_bounds() {
        let data = Data {
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

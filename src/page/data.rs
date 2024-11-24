use crate::Persistable;

#[derive(Debug)]
pub struct Data<const DATA_LENGTH: usize> {
    pub length: u32,
    pub data: [u8; DATA_LENGTH],
}

impl<const DATA_LENGTH: usize> Persistable for Data<DATA_LENGTH>
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        &self.data[..self.length as usize]
    }
}



#[derive(Debug)]
pub struct Data<const DATA_LENGTH: usize>  {
    pub data: [u8; DATA_LENGTH],
}
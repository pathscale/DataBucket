use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
pub enum DataType {
    String = 0,
    Integer = 1, // 64-bit integer
    Float = 2,   // 64-bit float
}

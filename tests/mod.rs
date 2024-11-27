use data_bucket::{align, SizeMeasurable, SizeMeasure};
use rkyv::{Archive, Serialize};

#[derive(SizeMeasure, Archive, Serialize)]
struct StringU {
    pub str: String,
    pub u: u16,
}

#[test]
fn test_string_u16() {
    let s = StringU {
        str: "123456789".to_string(),
        u: 2,
    };
    assert_eq!(s.aligned_size(), rkyv::to_bytes::<_, 0>(&s).unwrap().len())
}

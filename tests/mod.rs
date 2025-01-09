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
    assert_eq!(
        s.aligned_size(),
        rkyv::to_bytes::<rkyv::rancor::Error>(&s).unwrap().len()
    )
}

#[derive(SizeMeasure)]
struct MeasuredStruct {
    a: u32,
    b: u32,
}

#[test]
fn test_size_measure() {
    let measured_struct = MeasuredStruct {a: 3, b: 4};
    assert_eq!(measured_struct.aligned_size(), 8);
}

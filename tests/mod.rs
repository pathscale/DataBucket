use data_bucket::{align, SizeMeasurable, SizeMeasure};
use rkyv::{Archive, Serialize};

#[derive(SizeMeasure, Archive, Serialize)]
struct StringU {
    pub str: String,
    pub u: u16,
}

#[derive(SizeMeasure, Archive, Serialize)]
enum FixedState {
    Idle,
    Running,
    Complete,
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

#[test]
fn test_fieldless_enum_size() {
    for state in [FixedState::Idle, FixedState::Running, FixedState::Complete] {
        assert_eq!(
            state.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&state).unwrap().len()
        );
    }
}

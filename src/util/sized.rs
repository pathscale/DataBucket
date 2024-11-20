use crate::link::{Link, LINK_LENGTH};
use std::mem;
use uuid::Uuid;

pub const fn align(len: usize) -> usize {
    if len % 4 == 0 {
        len
    } else {
        (len / 4 + 1) * 4
    }
}

/// Marks an objects that can return theirs approximate size after archiving via
/// [`rkyv`].
pub trait SizeMeasurable {
    /// Returns approximate size of the object archiving via [`rkyv`].
    fn aligned_size(&self) -> usize;
}

macro_rules! size_measurable_for_sized {
    ($($t:ident),+) => {
        $(
            impl SizeMeasurable for $t {
                fn aligned_size(&self) -> usize {
                    mem::size_of::<$t>()
                }
            }
        )+
    };
}

size_measurable_for_sized! {u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool}

impl SizeMeasurable for Link {
    fn aligned_size(&self) -> usize {
        LINK_LENGTH
    }
}

impl SizeMeasurable for Uuid {
    fn aligned_size(&self) -> usize {
        16
    }
}

// That was found on practice... Check unit test for proofs that works.
impl SizeMeasurable for String {
    fn aligned_size(&self) -> usize {
        if self.len() < 8 {
            8
        } else if self.len() == 8 {
            16
        } else {
            align(self.len() + 8)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::util::sized::SizeMeasurable;

    #[test]
    fn test_string() {
        // Test if approximate size is correct for strings
        for i in 0..10_000 {
            let s = String::from_utf8(vec![b'a'; i]).unwrap();
            assert_eq!(s.aligned_size(), rkyv::to_bytes::<_, 0>(&s).unwrap().len())
        }
    }
}

use crate::link::{Link, LINK_LENGTH};
use ordered_float::OrderedFloat;
use rkyv::util::AlignedVec;
use std::{mem, sync::Arc};
use uuid::Uuid;

pub const fn align(len: usize) -> usize {
    if len % 4 == 0 {
        len
    } else {
        (len / 4 + 1) * 4
    }
}

pub const fn align8(len: usize) -> usize {
    if len % 8 == 0 {
        len
    } else {
        (len / 8 + 1) * 8
    }
}

pub fn align_vec<const ALIGNMENT: usize>(mut v: AlignedVec<ALIGNMENT>) -> AlignedVec<ALIGNMENT> {
    if v.len() != align(v.len()) {
        let count = align(v.len()) - v.len();
        for _ in 0..count {
            v.push(0)
        }
    }

    v
}

/// Marks an objects that can return theirs approximate size after archiving via
/// [`rkyv`].
pub trait SizeMeasurable {
    /// Returns approximate size of the object archiving via [`rkyv`].
    fn aligned_size(&self) -> usize;
    fn align() -> Option<usize> {
        None
    }
}

macro_rules! size_measurable_for_sized {
    ($($t:ident),+) => {
        $(
            impl SizeMeasurable for $t {
                fn aligned_size(&self) -> usize {
                    mem::size_of::<$t>()
                }
                fn align() -> Option<usize> {
                    Some(align(mem::size_of::<$t>()))
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

impl<T> SizeMeasurable for OrderedFloat<T>
where
    T: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        self.0.aligned_size()
    }
}

impl SizeMeasurable for [u8; 32] {
    fn aligned_size(&self) -> usize {
        mem::size_of::<[u8; 32]>()
    }
}

impl SizeMeasurable for [u8; 20] {
    fn aligned_size(&self) -> usize {
        mem::size_of::<[u8; 20]>()
    }
}

impl<T1, T2> SizeMeasurable for (T1, T2)
where
    T1: SizeMeasurable,
    T2: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        if let Some(align) = T1::align() {
            if align % 8 == 0 {
                return align8(self.0.aligned_size() + self.1.aligned_size());
            }
        }
        if let Some(align) = T2::align() {
            if align % 8 == 0 {
                return align8(self.0.aligned_size() + self.1.aligned_size());
            }
        }
        align(self.0.aligned_size() + self.1.aligned_size())
    }

    fn align() -> Option<usize> {
        if let Some(align) = T1::align() {
            if align % 8 == 0 {
                return Some(8);
            }
        }
        if let Some(align) = T2::align() {
            if align % 8 == 0 {
                return Some(8);
            }
        }
        None
    }
}

// That was found on practice... Check unit test for proofs that works.
impl SizeMeasurable for String {
    fn aligned_size(&self) -> usize {
        if self.len() <= 8 {
            8
        } else {
            align(self.len() + 8)
        }
    }
}

impl<T> SizeMeasurable for Vec<T>
where
    T: SizeMeasurable + Default,
{
    fn aligned_size(&self) -> usize {
        let val_size = T::default().aligned_size();
        let vec_content_size = if val_size == 2 {
            2
        } else if val_size == 4 {
            4
        } else if let Some(al) = T::align() {
            if al % 8 == 0 {
                align8(val_size)
            } else {
                val_size
            }
        } else {
            val_size
        };

        align(self.len() * vec_content_size) + 8
    }
}

impl<T: SizeMeasurable> SizeMeasurable for Arc<T> {
    fn aligned_size(&self) -> usize {
        self.as_ref().aligned_size()
    }
}

impl<T: SizeMeasurable> SizeMeasurable for lockfree::set::Set<T> {
    fn aligned_size(&self) -> usize {
        self.iter().map(|elem| elem.aligned_size()).sum()
    }
}

impl<T: SizeMeasurable> SizeMeasurable for Option<T>
where
    T: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        size_of::<Option<T>>()
    }
}

impl<K, V> SizeMeasurable for indexset::core::pair::Pair<K, V>
where
    K: SizeMeasurable,
    V: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        align(self.key.aligned_size() + self.value.aligned_size())
    }
}
impl<K, V> SizeMeasurable for indexset::core::multipair::MultiPair<K, V>
where
    K: SizeMeasurable,
    V: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        align(self.key.aligned_size() + self.value.aligned_size())
    }
}

/// Marks an objects that can return theirs approximate size after archiving via
/// [`rkyv`].
pub trait VariableSizeMeasurable {
    /// Returns approximate size of the object archiving via [`rkyv`].
    fn aligned_size(length: usize) -> usize;
}

impl VariableSizeMeasurable for String {
    fn aligned_size(length: usize) -> usize {
        if length <= 8 {
            8
        } else {
            align(length + 8)
        }
    }
}

impl<K> VariableSizeMeasurable for indexset::core::pair::Pair<K, Link>
where
    K: VariableSizeMeasurable,
{
    fn aligned_size(length: usize) -> usize {
        align(Link::default().aligned_size() + K::aligned_size(length))
    }
}
impl<K> VariableSizeMeasurable for indexset::core::multipair::MultiPair<K, Link>
where
    K: VariableSizeMeasurable,
{
    fn aligned_size(length: usize) -> usize {
        align(Link::default().aligned_size() + K::aligned_size(length))
    }
}

#[cfg(test)]
mod test {
    use crate::util::sized::SizeMeasurable;
    use crate::{IndexValue, Link};
    use rkyv::to_bytes;
    use uuid::Uuid;

    #[test]
    fn test_uuid() {
        let u = Uuid::new_v4();
        assert_eq!(
            u.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&u).unwrap().len()
        );
        let t = (Uuid::new_v4(), Link::default());
        assert_eq!(
            t.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        );
        let v = IndexValue {
            key: u,
            link: Default::default(),
        };
        assert_eq!(
            v.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&v).unwrap().len()
        );
        let mut vec = Vec::new();
        vec.push(IndexValue {
            key: Uuid::new_v4(),
            link: Default::default(),
        });
        assert_eq!(
            vec.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&vec).unwrap().len()
        );
        for _ in 0..600 {
            vec.push(IndexValue {
                key: Uuid::new_v4(),
                link: Default::default(),
            })
        }
        assert_eq!(
            vec.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&vec).unwrap().len()
        )
    }

    #[test]
    fn test_tuple() {
        let t = (u64::MAX, Link::default());
        assert_eq!(
            t.aligned_size(),
            to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        );
        let t = (u32::MAX, Link::default());
        assert_eq!(
            t.aligned_size(),
            to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        );
        let t = (u8::MAX, Link::default());
        assert_eq!(
            t.aligned_size(),
            to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        )
    }

    #[test]
    fn test_string() {
        // Test if approximate size is correct for strings
        for i in 0..10_000 {
            let s = String::from_utf8(vec![b'a'; i]).unwrap();
            assert_eq!(
                s.aligned_size(),
                rkyv::to_bytes::<rkyv::rancor::Error>(&s).unwrap().len()
            )
        }
    }

    #[test]
    fn test_index_value_str() {
        // Test if approximate size is correct for strings
        for i in 0..10_000 {
            let s = String::from_utf8(vec![b'a'; i]).unwrap();
            let v = IndexValue {
                key: s,
                link: Default::default(),
            };
            assert_eq!(
                v.aligned_size(),
                rkyv::to_bytes::<rkyv::rancor::Error>(&v).unwrap().len()
            )
        }
    }
}

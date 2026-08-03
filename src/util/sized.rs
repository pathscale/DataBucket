use crate::link::{Link, LINK_LENGTH};
use ordered_float::OrderedFloat;
use psc_nanoid::packed::AlphabetPackExt;
use psc_nanoid::PackedNanoid;
use rkyv::util::AlignedVec;
use std::{mem, sync::Arc};
use uuid::Uuid;

pub const fn align(len: usize) -> usize {
    if len.is_multiple_of(4) {
        len
    } else {
        (len / 4 + 1) * 4
    }
}

pub const fn align8(len: usize) -> usize {
    if len.is_multiple_of(8) {
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

    /// Returns the archived size of this type's default value.
    ///
    /// The default implementation constructs `Self::default()`. Types with an
    /// expensive default may override this method in their `SizeMeasurable`
    /// implementation and return the size directly.
    fn default_aligned_size() -> usize
    where
        Self: Default,
    {
        Self::default().aligned_size()
    }

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

macro_rules! size_measurable_for_primitive_arrays {
    ($($t:ident),+) => {
        $(
            impl<const N: usize> SizeMeasurable for [$t; N] {
                fn aligned_size(&self) -> usize {
                    mem::size_of::<[$t; N]>()
                }
            }
        )+
    };
}

// Deliberately retain the previous `align() == None` behavior of the existing
// `[u8; 20]` and `[u8; 32]` implementations. Changing that value can alter
// persisted tuple and page offsets.
size_measurable_for_primitive_arrays! {u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool}

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

impl<const N: usize, const B: usize, A: AlphabetPackExt> SizeMeasurable for PackedNanoid<N, B, A> {
    fn aligned_size(&self) -> usize {
        B
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
    T: Default + SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        let val_size = <T as SizeMeasurable>::default_aligned_size();
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

impl<T: SizeMeasurable> SizeMeasurable for Option<T>
where
    T: SizeMeasurable,
{
    fn align() -> Option<usize> {
        T::align()
    }

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

impl<K, L> VariableSizeMeasurable for indexset::core::pair::Pair<K, L>
where
    K: VariableSizeMeasurable,
    L: Default + SizeMeasurable,
{
    fn aligned_size(length: usize) -> usize {
        align(<L as SizeMeasurable>::default_aligned_size() + K::aligned_size(length))
    }
}
impl<K, L> VariableSizeMeasurable for indexset::core::multipair::MultiPair<K, L>
where
    K: VariableSizeMeasurable,
    L: Default + SizeMeasurable,
{
    fn aligned_size(length: usize) -> usize {
        align(<L as SizeMeasurable>::default_aligned_size() + K::aligned_size(length))
    }
}

#[cfg(test)]
mod test {
    use crate::util::sized::SizeMeasurable;
    use crate::{IndexValue, Link};
    use rkyv::to_bytes;
    use uuid::Uuid;

    #[test]
    fn primitive_arrays_preserve_existing_alignment_contract() {
        assert_eq!([0u8; 20].aligned_size(), 20);
        assert_eq!([0u8; 32].aligned_size(), 32);
        assert_eq!(<[u8; 20] as SizeMeasurable>::align(), None);
        assert_eq!(<[u8; 32] as SizeMeasurable>::align(), None);

        let values = [1u16, 2, 3];
        assert_eq!(
            values.aligned_size(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&values)
                .unwrap()
                .len()
        );
    }

    #[test]
    fn default_size_helper_matches_previous_expression() {
        assert_eq!(
            <u64 as SizeMeasurable>::default_aligned_size(),
            u64::default().aligned_size()
        );
    }

    #[test]
    fn type_can_override_default_size_without_constructing_default() {
        struct ExpensiveDefault;

        impl Default for ExpensiveDefault {
            fn default() -> Self {
                panic!("the sizing override must not construct the default value")
            }
        }

        impl SizeMeasurable for ExpensiveDefault {
            fn aligned_size(&self) -> usize {
                64
            }

            fn default_aligned_size() -> usize {
                64
            }
        }

        assert_eq!(
            <ExpensiveDefault as SizeMeasurable>::default_aligned_size(),
            64
        );
    }

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
        );
        let t = (Some(0.0f64), Link::default());
        assert_eq!(
            t.aligned_size(),
            to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        )
    }
    #[test]
    fn test_option() {
        let t = Some(0.0f64);
        assert_eq!(
            t.aligned_size(),
            to_bytes::<rkyv::rancor::Error>(&t).unwrap().len()
        );
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

    #[test]
    fn test_packed_nanoid() {
        use psc_nanoid::{alphabet::Base64UrlAlphabet, packed::PackedNanoid, Nanoid};

        fn check<const N: usize, const B: usize>() {
            let id = Nanoid::<N, Base64UrlAlphabet>::new();
            let packed = PackedNanoid::<N, B, Base64UrlAlphabet>::pack(&id).unwrap();
            assert_eq!(
                packed.aligned_size(),
                rkyv::to_bytes::<rkyv::rancor::Error>(&packed)
                    .unwrap()
                    .len()
            );
        }

        // For Base64UrlAlphabet, PACK_BITS = 6, so B = ceil(N * 6 / 8)
        check::<1, 1>();
        check::<6, 5>();
        check::<10, 8>();
        check::<21, 16>();
        check::<32, 24>();
        check::<42, 32>();
        check::<64, 48>();
    }
}

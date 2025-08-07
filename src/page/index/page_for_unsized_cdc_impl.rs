use eyre::bail;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    IndexValue, Link, SizeMeasurable, UnsizedIndexPage, UnsizedIndexPageUtility,
    VariableSizeMeasurable,
};

impl<T, const DATA_LENGTH: u32> UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + Default
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    pub fn apply_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                event_id: _,
                max_value: _,
                value,
                index,
            } => {
                if value.key > self.node_id.key {
                    self.node_id = value.clone().into();
                    self.node_id_size = value.aligned_size() as u16;
                }
                if index == self.slots_size as usize {
                    self.node_id = value.clone().into();
                    self.node_id_size = value.aligned_size() as u16;
                }
                self.apply_insert_at(index, value)?;
                Ok(())
            }
            ChangeEvent::RemoveAt {
                event_id: _,
                max_value,
                value,
                index,
            } => {
                // we are checking if index is non-zero because for non-unique indexes this is possible and will
                // lead to panic, but in this case new node_id will be same to current node_id so it's change
                // will not affect at all.
                if value == max_value && index != 0 {
                    let new_node_id = self
                        .index_values
                        .get(index - 1)
                        .expect("should be available");
                    self.node_id = new_node_id.clone();
                    self.node_id_size = new_node_id.aligned_size() as u16;
                }
                self.apply_remove_at(index)?;
                Ok(())
            }
            ChangeEvent::SplitNode { .. }
            | ChangeEvent::CreateNode { .. }
            | ChangeEvent::RemoveNode { .. } => {
                bail!("Events of `SplitNode`, `CreateNode` or `RemoveNode` can not be applied")
            }
        }
    }

    fn apply_insert_at(&mut self, index: usize, value: Pair<T, Link>) -> eyre::Result<()> {
        // For insert we first add slot entry for our new index value
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        let bytes = rkyv::to_bytes(&index_value)?;
        let value_offset = self.last_value_offset;
        let len = bytes.len();
        self.last_value_offset += bytes.len() as u32;
        if index == self.index_values.len() {
            self.index_values.push(index_value.clone());
        } else {
            self.index_values.insert(index, index_value.clone());
        }

        self.slots
            .insert(index, (value_offset + len as u32, len as u16));
        self.slots_size += 1;

        Ok(())
    }

    fn apply_remove_at(&mut self, index: usize) -> eyre::Result<()> {
        self.slots.remove(index);
        self.slots_size -= 1;
        let v = self.index_values.remove(index);

        self.removed_len +=
            (v.aligned_size() + UnsizedIndexPageUtility::<T>::slots_value_size()) as u32;

        if self.removed_len > DATA_LENGTH / 2 {
            self.rebuild();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{IndexValue, Link, UnsizedIndexPage};
    use indexset::cdc::change::ChangeEvent;
    use indexset::core::pair::Pair;

    #[test]
    fn test_insert_at() {
        let mut page = UnsizedIndexPage::<_, 1024>::new(IndexValue {
            key: "Something".to_string(),
            link: Default::default(),
        })
        .unwrap();
        let event = ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something".to_string(),
                value: Link::default(),
            },
            value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            index: 1,
        };
        page.apply_change_event(event).unwrap();

        assert_eq!(page.node_id.key, "Something new".to_string());
    }

    #[test]
    fn test_remove_at() {
        let mut page = UnsizedIndexPage::<_, 1024>::new(IndexValue {
            key: "Something".to_string(),
            link: Default::default(),
        })
        .unwrap();
        let event = ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something".to_string(),
                value: Link::default(),
            },
            value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            index: 1,
        };
        page.apply_change_event(event).unwrap();
        let event = ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            value: Pair {
                key: "Something".to_string(),
                value: Link::default(),
            },
            index: 0,
        };
        page.apply_change_event(event).unwrap();

        assert_eq!(page.node_id.key, "Something new".to_string());
    }

    #[test]
    fn test_remove_at_node_id() {
        let mut page = UnsizedIndexPage::<_, 1024>::new(IndexValue {
            key: "Something".to_string(),
            link: Default::default(),
        })
        .unwrap();
        let event = ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something".to_string(),
                value: Link::default(),
            },
            value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            index: 1,
        };
        page.apply_change_event(event).unwrap();
        let event = ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            value: Pair {
                key: "Something new".to_string(),
                value: Link::default(),
            },
            index: 1,
        };
        page.apply_change_event(event).unwrap();

        assert_eq!(page.node_id.key, "Something".to_string());
    }
}

use crate::{IndexPage, IndexPageUtility, IndexValue, Link, SizeMeasurable};
use eyre::{bail, eyre};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

impl<T: Default + SizeMeasurable> IndexPage<T>
where
    T: Archive
        + Clone
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + PartialEq
        + PartialOrd
        + Send
        + Sync,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    pub fn apply_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                max_value,
                value,
                index,
            } => {
                if value.key > self.node_id {
                    self.node_id = value.key.clone();
                }
                self.apply_insert_at(index, value)?;
                Ok(())
            }
            ChangeEvent::RemoveAt { .. } => Ok(()),
            ChangeEvent::SplitNode { .. } => Ok(()),
            ChangeEvent::CreateNode { .. } | ChangeEvent::RemoveNode { .. } => {
                bail!("Events of `CreateNode` and `RemoveNode` can not be applied")
            }
        }
    }

    fn apply_insert_at(&mut self, index: usize, value: Pair<T, Link>) -> eyre::Result<()> {
        // For insert we first add slot entry for our new index value
        self.slots.insert(index, self.current_index);
        self.slots.remove(self.size as usize);
        self.current_length += 1;
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        // After we insert index value in array at position selected with `current_index` value
        let mut value_position = self.current_index;
        self.index_values[value_position as usize] = index_value;
        // After we need to find next empty slot to point with `current_index` value
        // If we were not inserting at last slot in array
        if value_position != self.size - 1 {
            // We need to iterate over next values to find empty one.
            // (We do this because after remove, `current_index` value can point at removed slot)
            value_position += 1;
            let mut value = &self.index_values[value_position as usize];
            while value != &IndexValue::default() && value_position != self.size - 1 {
                value_position += 1;
                value = &self.index_values[value_position as usize];
            }
            self.current_index = value_position;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{IndexPage, IndexValue, Link};
    use indexset::cdc::change::ChangeEvent;
    use indexset::core::pair::Pair;

    #[test]
    fn test_insert_at() {
        let mut page = IndexPage::new(1, 10);
        let event = ChangeEvent::InsertAt {
            max_value: Pair {
                key: 1,
                value: Link::default(),
            },
            value: Pair {
                key: 1,
                value: Link::default(),
            },
            index: 0,
        };
        page.apply_change_event(event).unwrap();

        assert_eq!(page.node_id, 1);
        assert_eq!(
            page.index_values[0],
            IndexValue {
                key: 1,
                link: Link::default(),
            }
        )
    }

    #[test]
    fn test_insert_at_second() {
        let mut page = IndexPage::new(1, 10);
        let event = ChangeEvent::InsertAt {
            max_value: Pair {
                key: 1,
                value: Link::default(),
            },
            value: Pair {
                key: 1,
                value: Link::default(),
            },
            index: 0,
        };
        page.apply_change_event(event).unwrap();
        let event = ChangeEvent::InsertAt {
            max_value: Pair {
                key: 1,
                value: Link::default(),
            },
            value: Pair {
                key: 2,
                value: Link::default(),
            },
            index: 0,
        };
        page.apply_change_event(event).unwrap();

        assert_eq!(page.node_id, 2);
        assert_eq!(
            page.index_values[0],
            IndexValue {
                key: 1,
                link: Link::default(),
            }
        );
        assert_eq!(
            page.index_values[1],
            IndexValue {
                key: 2,
                link: Link::default(),
            }
        )
    }
}

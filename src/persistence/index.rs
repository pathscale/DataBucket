use crate::page::IndexPage;
use crate::SizeMeasurable;

pub trait PersistableIndex {
    type PersistedIndex;

    fn get_index_names(&self) -> Vec<&str>;

    fn get_pages_by_name<T>(&self, name: &str) -> Vec<IndexPage<T>>
    where
        T: Clone + Ord + SizeMeasurable + 'static;

    fn get_persisted_index(&self) -> Self::PersistedIndex;
}

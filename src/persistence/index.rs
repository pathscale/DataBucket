use crate::page::IndexPage;

pub trait PersistIndex {
    type PersistedIndex;

    fn get_index_names(&self) -> Vec<&str>;

    fn get_pages_by_name<T>(&self, name: &str) -> Vec<IndexPage<T>>;

    fn get_persisted_index(&self) -> Self::PersistedIndex;
}

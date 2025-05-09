pub trait PersistableIndex {
    type PersistedIndex;

    fn get_persisted_index(&self) -> Self::PersistedIndex;

    fn from_persisted(persisted: Self::PersistedIndex) -> Self;
}

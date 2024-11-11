pub trait PersistableTable {
    const TABLE_NAME: &'static str;
    type Space;

    fn get_space(&self) -> Self::Space;
}

//! [`SpaceInfoPage`] declaration.

use crate::util::Persistable;
use crate::{space, Link};

use data_bucket_codegen::Persistable;
use rkyv::{Archive, Deserialize, Serialize};

pub type SpaceName = String;

/// Legacy SpaceInfoPage format (version 1) - no version field.
/// Used for migrating existing data files.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct SpaceInfoPageV1<Pk = ()> {
    pub id: space::Id,
    pub page_count: u32,
    pub pk_gen_state: Pk,
    pub name: SpaceName,
    pub row_schema: Vec<(String, String)>,
    pub primary_key_fields: Vec<String>,
    pub secondary_index_types: Vec<(String, String)>,
    pub empty_links_list: Vec<Link>,
}

impl<Pk> From<SpaceInfoPageV1<Pk>> for SpaceInfoPage<Pk> {
    fn from(v1: SpaceInfoPageV1<Pk>) -> Self {
        SpaceInfoPage {
            version: 0,
            id: v1.id,
            page_count: v1.page_count,
            pk_gen_state: v1.pk_gen_state,
            name: v1.name,
            row_schema: v1.row_schema,
            primary_key_fields: v1.primary_key_fields,
            secondary_index_types: v1.secondary_index_types,
            empty_links_list: v1.empty_links_list,
        }
    }
}

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize, Persistable)]
pub struct SpaceInfoPage<Pk = ()> {
    pub id: space::Id,
    pub page_count: u32,
    pub pk_gen_state: Pk,
    pub name: SpaceName,
    pub version: u32,
    pub row_schema: Vec<(String, String)>,
    pub primary_key_fields: Vec<String>,
    pub secondary_index_types: Vec<(String, String)>,
    pub empty_links_list: Vec<Link>,
}

/// Represents some interval between values.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Interval(pub usize, pub usize);

impl Interval {
    pub fn contains(&self, interval: &Interval) -> bool {
        self.0 <= interval.0 && self.1 >= interval.1
    }
}

#[cfg(test)]
mod test {
    use crate::page::{SpaceInfoPage, INNER_PAGE_SIZE};
    use crate::util::Persistable;

    #[test]
    fn test_as_bytes() {
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            version: 1,
            row_schema: vec![],
            primary_key_fields: vec![],
            pk_gen_state: 0u128,
            empty_links_list: vec![],
            secondary_index_types: vec![],
        };
        let bytes = info.as_bytes();
        assert!(bytes.as_ref().len() < INNER_PAGE_SIZE)
    }
}

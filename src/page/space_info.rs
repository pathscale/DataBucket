//! [`SpaceInfoPage`] declaration.

use crate::util::Persistable;
use crate::{space, Link};

use data_bucket_codegen::Persistable;
use rkyv::{Archive, Deserialize, Serialize};

pub type SpaceName = String;

/// Legacy SpaceInfoPage format (version 1) - no version field.
/// Used for migrating existing data files.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize, Persistable)]
pub(crate) struct SpaceInfoPageV1<Pk = ()> {
    pub id: space::Id,
    pub page_count: u32,
    pub pk_gen_state: Pk,
    pub name: SpaceName,
    pub row_schema: Vec<(String, String)>,
    pub primary_key_fields: Vec<String>,
    pub secondary_index_types: Vec<(String, String)>,
    pub empty_links_list: Vec<Link>,
}

/// Current SpaceInfoPage format (version 2+) - with version field.
/// Internal struct for serialization, converted to public SpaceInfoPage.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize, Persistable)]
pub(crate) struct SpaceInfoPageV2<Pk = ()> {
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

impl<Pk> From<SpaceInfoPageV2<Pk>> for SpaceInfoPage<Pk> {
    fn from(v2: SpaceInfoPageV2<Pk>) -> Self {
        SpaceInfoPage {
            version: v2.version,
            id: v2.id,
            page_count: v2.page_count,
            pk_gen_state: v2.pk_gen_state,
            name: v2.name,
            row_schema: v2.row_schema,
            primary_key_fields: v2.primary_key_fields,
            secondary_index_types: v2.secondary_index_types,
            empty_links_list: v2.empty_links_list,
        }
    }
}

impl<Pk: Clone> From<SpaceInfoPage<Pk>> for SpaceInfoPageV2<Pk> {
    fn from(page: SpaceInfoPage<Pk>) -> Self {
        SpaceInfoPageV2 {
            version: page.version,
            id: page.id,
            page_count: page.page_count,
            pk_gen_state: page.pk_gen_state,
            name: page.name,
            row_schema: page.row_schema,
            primary_key_fields: page.primary_key_fields,
            secondary_index_types: page.secondary_index_types,
            empty_links_list: page.empty_links_list,
        }
    }
}

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
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

impl<Pk> Persistable for SpaceInfoPage<Pk>
where
    Pk: Archive
        + Clone
        + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    <Pk as Archive>::Archived: rkyv::Deserialize<Pk, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> + Send {
        let v2 = SpaceInfoPageV2 {
            version: self.version,
            id: self.id,
            page_count: self.page_count,
            pk_gen_state: self.pk_gen_state.clone(),
            name: self.name.clone(),
            row_schema: self.row_schema.clone(),
            primary_key_fields: self.primary_key_fields.clone(),
            secondary_index_types: self.secondary_index_types.clone(),
            empty_links_list: self.empty_links_list.clone(),
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&v2).unwrap()
    }

    fn from_bytes(bytes: &[u8], version: u32) -> Self {
        match version {
            1 => {
                let v1 = SpaceInfoPageV1::from_bytes(bytes, version);
                v1.into()
            }
            _ => {
                let v2 = SpaceInfoPageV2::from_bytes(bytes, version);
                v2.into()
            }
        }
    }
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
    use super::{SpaceInfoPage, SpaceInfoPageV1, SpaceInfoPageV2};
    use crate::page::INNER_PAGE_SIZE;
    use crate::util::Persistable;
    use rkyv::Archive;

    #[test]
    fn test_as_bytes() {
        let info: SpaceInfoPage = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            version: 1,
            row_schema: vec![],
            primary_key_fields: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
            secondary_index_types: vec![],
        };
        let bytes = info.as_bytes();
        assert!(bytes.as_ref().len() < INNER_PAGE_SIZE)
    }

    #[test]
    fn test_migration_from_v1() {
        let old_info: SpaceInfoPageV1 = SpaceInfoPageV1 {
            id: 42.into(),
            page_count: 5,
            pk_gen_state: (),
            name: "legacy_table".to_string(),
            row_schema: vec![("col1".to_string(), "i32".to_string())],
            primary_key_fields: vec!["col1".to_string()],
            secondary_index_types: vec![],
            empty_links_list: vec![],
        };

        let migrated: SpaceInfoPage = old_info.into();

        assert_eq!(migrated.version, 0);
        assert_eq!(migrated.id, 42.into());
        assert_eq!(migrated.page_count, 5);
        assert_eq!(migrated.name, "legacy_table");
        assert_eq!(migrated.row_schema, vec![("col1".to_string(), "i32".to_string())]);
        assert_eq!(migrated.primary_key_fields, vec!["col1".to_string()]);
    }

    #[test]
    fn test_v1_bytes_roundtrip_and_migration() {
        let old_info: SpaceInfoPageV1 = SpaceInfoPageV1 {
            id: 10.into(),
            page_count: 3,
            pk_gen_state: (),
            name: "old_data".to_string(),
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            empty_links_list: vec![],
        };
        let bytes = old_info.as_bytes();

        let archived = unsafe {
            rkyv::access_unchecked::<<SpaceInfoPageV1 as Archive>::Archived>(bytes.as_ref())
        };
        let deserialized: SpaceInfoPageV1 =
            rkyv::deserialize::<_, rkyv::rancor::Error>(archived).unwrap();

        let migrated: SpaceInfoPage = deserialized.into();

        assert_eq!(migrated.version, 0);
        assert_eq!(migrated.id, 10.into());
        assert_eq!(migrated.name, "old_data");
    }

    #[test]
    fn test_v2_bytes_roundtrip() {
        let info: SpaceInfoPageV2 = SpaceInfoPageV2 {
            id: 20.into(),
            page_count: 7,
            pk_gen_state: (),
            name: "new_table".to_string(),
            version: 2,
            row_schema: vec![("col2".to_string(), "String".to_string())],
            primary_key_fields: vec!["col2".to_string()],
            secondary_index_types: vec![],
            empty_links_list: vec![],
        };
        let bytes = info.as_bytes();

        let archived = unsafe {
            rkyv::access_unchecked::<<SpaceInfoPageV2 as Archive>::Archived>(bytes.as_ref())
        };
        let deserialized: SpaceInfoPageV2 =
            rkyv::deserialize::<_, rkyv::rancor::Error>(archived).unwrap();

        assert_eq!(deserialized.version, 2);
        assert_eq!(deserialized.id, 20.into());
        assert_eq!(deserialized.name, "new_table");
    }

    #[test]
    fn test_persistable_version_handling() {
        let v1_info: SpaceInfoPageV1 = SpaceInfoPageV1 {
            id: 100.into(),
            page_count: 10,
            pk_gen_state: (),
            name: "v1_table".to_string(),
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            empty_links_list: vec![],
        };
        let v1_bytes = v1_info.as_bytes();

        let page_from_v1: SpaceInfoPage = SpaceInfoPage::from_bytes(v1_bytes.as_ref(), 1);
        assert_eq!(page_from_v1.version, 0);
        assert_eq!(page_from_v1.id, 100.into());

        let v2_info: SpaceInfoPageV2 = SpaceInfoPageV2 {
            id: 200.into(),
            page_count: 20,
            pk_gen_state: (),
            name: "v2_table".to_string(),
            version: 5,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            empty_links_list: vec![],
        };
        let v2_bytes = v2_info.as_bytes();

        let page_from_v2: SpaceInfoPage = SpaceInfoPage::from_bytes(v2_bytes.as_ref(), 2);
        assert_eq!(page_from_v2.version, 5);
        assert_eq!(page_from_v2.id, 200.into());
    }
}
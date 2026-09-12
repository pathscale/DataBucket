//! Database-wide durability generations and their generated catalog boundary.
//!
//! DataBucket owns the physical generation protocol. The catalog implementation
//! is supplied by WorkTable and is a generated table. Keeping that dependency
//! behind this interface avoids a Cargo cycle without replacing the table with
//! a second hand-written database engine.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;
use core::error::Error as CoreError;
use core::fmt::{Display, Formatter};

use crate::page::PageId;
use crate::SpaceId;

#[cfg(feature = "s3-support")]
pub mod s3;

pub const CATALOG_NAME_BYTES: usize = 96;
pub const OBJECT_ID_BYTES: usize = 32;

pub type CatalogKey = [u8; 32];
pub type ObjectId = [u8; OBJECT_ID_BYTES];
pub type Checksum = [u8; 32];
pub type Generation = u64;
pub type WriterEpoch = u64;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageDomainId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CatalogRecordKind {
    Table = 1,
    Page = 2,
    Index = 3,
    Replication = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PageKind {
    Data = 1,
    PrimaryIndex = 2,
    SecondaryIndex = 3,
    Metadata = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ReplicaState {
    Absent = 0,
    Staged = 1,
    Durable = 2,
    Failed = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum ReplicationErrorCode {
    Transport = 1,
    Conflict = 2,
    Corrupt = 3,
    Unauthorized = 4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogName {
    length: u8,
    bytes: [u8; CATALOG_NAME_BYTES],
}

impl CatalogName {
    pub fn new(name: &str) -> Result<Self, CatalogError> {
        let length = u8::try_from(name.len()).map_err(|_| CatalogError::NameTooLong)?;
        if name.len() > CATALOG_NAME_BYTES {
            return Err(CatalogError::NameTooLong);
        }
        let mut bytes = [0; CATALOG_NAME_BYTES];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Ok(Self { length, bytes })
    }

    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.bytes[..usize::from(self.length)])
            .expect("CatalogName is constructed from UTF-8")
    }

    pub fn length(&self) -> u8 {
        self.length
    }

    pub fn bytes(&self) -> &[u8; CATALOG_NAME_BYTES] {
        &self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemTableRecord {
    pub table_id: TableId,
    pub name: CatalogName,
    pub schema_version: u32,
    pub data_space_id: SpaceId,
    pub page_stride: u32,
    pub row_count: u64,
    pub live_row_bytes: u64,
    pub allocated_data_pages: u64,
    pub live_data_pages: u64,
    pub primary_index_entries: u64,
    pub secondary_index_entries: u64,
    pub tombstones: u64,
    pub applied_generation: Generation,
    pub durable_generation: Generation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemPageRecord {
    pub table_id: TableId,
    pub space_id: SpaceId,
    pub page_id: PageId,
    pub page_kind: PageKind,
    pub generation: Generation,
    pub object: ObjectId,
    pub object_offset: u64,
    pub encoded_length: u32,
    pub decoded_length: u32,
    pub checksum: Checksum,
    pub live_rows: u32,
    pub live_bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemIndexRecord {
    pub table_id: TableId,
    pub index_id: u32,
    pub space_id: SpaceId,
    pub primary: bool,
    pub name: CatalogName,
    pub entries: u64,
    pub generation: Generation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemReplicationRecord {
    pub generation: Generation,
    pub upstash: ReplicaState,
    pub tigris: ReplicaState,
    pub last_error: Option<ReplicationErrorCode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogRecord {
    Table(SystemTableRecord),
    Page(SystemPageRecord),
    Index(SystemIndexRecord),
    Replication(SystemReplicationRecord),
}

impl CatalogRecord {
    pub fn kind(&self) -> CatalogRecordKind {
        match self {
            Self::Table(_) => CatalogRecordKind::Table,
            Self::Page(_) => CatalogRecordKind::Page,
            Self::Index(_) => CatalogRecordKind::Index,
            Self::Replication(_) => CatalogRecordKind::Replication,
        }
    }

    pub fn key(&self) -> CatalogKey {
        let mut key = [0; 32];
        key[0] = self.kind() as u8;
        match self {
            Self::Table(row) => key[1..5].copy_from_slice(&row.table_id.0.to_be_bytes()),
            Self::Page(row) => {
                key[1..5].copy_from_slice(&row.table_id.0.to_be_bytes());
                key[5..9].copy_from_slice(&row.space_id.0.to_be_bytes());
                let page: usize = row.page_id.into();
                key[9..13].copy_from_slice(&(page as u32).to_be_bytes());
            }
            Self::Index(row) => {
                key[1..5].copy_from_slice(&row.table_id.0.to_be_bytes());
                key[5..9].copy_from_slice(&row.index_id.to_be_bytes());
            }
            Self::Replication(row) => key[1..9].copy_from_slice(&row.generation.to_be_bytes()),
        }
        key
    }

    pub fn table_key(table_id: TableId) -> CatalogKey {
        let mut key = [0; 32];
        key[0] = CatalogRecordKind::Table as u8;
        key[1..5].copy_from_slice(&table_id.0.to_be_bytes());
        key
    }

    pub fn page_key(address: PageAddress) -> CatalogKey {
        let mut key = [0; 32];
        key[0] = CatalogRecordKind::Page as u8;
        key[1..5].copy_from_slice(&address.table_id.0.to_be_bytes());
        key[5..9].copy_from_slice(&address.space_id.0.to_be_bytes());
        let page: usize = address.page_id.into();
        key[9..13].copy_from_slice(&(page as u32).to_be_bytes());
        key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogMutation {
    Upsert(CatalogRecord),
    Delete(CatalogKey),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageAddress {
    pub domain: StorageDomainId,
    pub table_id: TableId,
    pub space_id: SpaceId,
    pub page_id: PageId,
    pub page_kind: PageKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Put {
        image: Vec<u8>,
        live_rows: u32,
        live_bytes: u32,
    },
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageMutation {
    pub address: PageAddress,
    pub kind: MutationKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationPlan {
    pub domain: StorageDomainId,
    pub id: Generation,
    pub parent: Generation,
    pub writer_epoch: WriterEpoch,
    pub pages: Vec<PageMutation>,
    pub catalog_delta: Vec<CatalogMutation>,
}

/// Collects every physical and catalog change that becomes visible together.
pub struct GenerationBuilder {
    plan: GenerationPlan,
}

impl GenerationBuilder {
    pub fn put_page(
        &mut self,
        address: PageAddress,
        image: Vec<u8>,
        live_rows: u32,
        live_bytes: u32,
    ) -> &mut Self {
        self.plan.pages.push(PageMutation {
            address,
            kind: MutationKind::Put {
                image,
                live_rows,
                live_bytes,
            },
        });
        self
    }

    pub fn delete_page(&mut self, address: PageAddress) -> &mut Self {
        self.plan.pages.push(PageMutation {
            address,
            kind: MutationKind::Delete,
        });
        self
    }

    pub fn update_catalog(&mut self, mutation: CatalogMutation) -> &mut Self {
        self.plan.catalog_delta.push(mutation);
        self
    }

    pub fn finish(self) -> GenerationPlan {
        self.plan
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRef {
    pub object: ObjectId,
    pub offset: u64,
    pub encoded_length: u32,
    pub decoded_length: u32,
    pub checksum: Checksum,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRef {
    pub address: PageAddress,
    pub object: ObjectRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedPage {
    pub address: PageAddress,
    pub object: ObjectRef,
    pub live_rows: u32,
    pub live_bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Head {
    pub domain: StorageDomainId,
    pub generation: Generation,
    pub parent: Generation,
    pub writer_epoch: WriterEpoch,
    pub catalog: ObjectRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedGeneration {
    pub domain: StorageDomainId,
    pub generation: Generation,
    pub parent: Generation,
    pub writer_epoch: WriterEpoch,
    pub pages: Vec<StagedPage>,
    pub catalog: Option<ObjectRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedGeneration {
    pub head: Head,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogError {
    NameTooLong,
    Codec,
    InvalidMutation,
}

impl Display for CatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NameTooLong => write!(
                formatter,
                "system catalog name exceeds {CATALOG_NAME_BYTES} bytes"
            ),
            Self::Codec => write!(formatter, "generated system catalog checkpoint is invalid"),
            Self::InvalidMutation => write!(formatter, "system catalog mutation is invalid"),
        }
    }
}

impl CoreError for CatalogError {}

pub struct CatalogWritePermit {
    _private: (),
}

pub trait PreparedSystemCatalog {
    fn checkpoint(&self) -> &[u8];
}

pub trait SystemCatalog: Send + Sync {
    type Prepared: PreparedSystemCatalog;

    fn prepare(
        &self,
        permit: &CatalogWritePermit,
        mutations: &[CatalogMutation],
    ) -> Result<Self::Prepared, CatalogError>;

    fn prepare_restore(
        &self,
        permit: &CatalogWritePermit,
        checkpoint: &[u8],
    ) -> Result<Self::Prepared, CatalogError>;

    fn publish(&self, permit: &CatalogWritePermit, prepared: Self::Prepared);

    fn record(&self, key: &CatalogKey) -> Option<CatalogRecord>;

    fn records(&self, kind: CatalogRecordKind) -> Vec<CatalogRecord>;
}

pub trait PageStore {
    type Error: CoreError + Send + Sync + 'static;

    fn load_head(&self, domain: StorageDomainId) -> Result<Option<Head>, Self::Error>;

    fn load_catalog(&self, head: &Head) -> Result<Vec<u8>, Self::Error>;

    fn read_page(&self, page: &PageRef) -> Result<Vec<u8>, Self::Error>;

    fn read_pages(&self, pages: &[PageRef]) -> Result<Vec<Vec<u8>>, Self::Error> {
        pages.iter().map(|page| self.read_page(page)).collect()
    }

    fn stage(&self, plan: &GenerationPlan) -> Result<StagedGeneration, Self::Error>;

    fn stage_catalog(
        &self,
        staged: &mut StagedGeneration,
        checkpoint: &[u8],
    ) -> Result<(), Self::Error>;

    fn commit(&self, staged: StagedGeneration) -> Result<CommittedGeneration, Self::Error>;
}

#[derive(Debug)]
pub enum DomainError<E> {
    StaleParent {
        expected: Generation,
        found: Generation,
    },
    WrongDomain,
    StaleWriter,
    InvalidGeneration,
    InvalidStoreResponse,
    Page(crate::error::Error),
    MissingCatalogObject,
    Catalog(CatalogError),
    Store(E),
}

impl<E: Display> Display for DomainError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::StaleParent { expected, found } => {
                write!(
                    formatter,
                    "generation parent is {found}, expected {expected}"
                )
            }
            Self::WrongDomain => {
                write!(formatter, "page mutation belongs to another storage domain")
            }
            Self::StaleWriter => write!(formatter, "generation belongs to another writer epoch"),
            Self::InvalidGeneration => write!(formatter, "generation must advance by exactly one"),
            Self::InvalidStoreResponse => {
                write!(formatter, "page store returned an inconsistent generation")
            }
            Self::Page(error) => error.fmt(formatter),
            Self::MissingCatalogObject => {
                write!(formatter, "page store did not stage the system catalog")
            }
            Self::Catalog(error) => error.fmt(formatter),
            Self::Store(error) => error.fmt(formatter),
        }
    }
}

impl<E: CoreError + 'static> CoreError for DomainError<E> {}

pub struct StorageDomain<C, S> {
    id: StorageDomainId,
    generation: Generation,
    writer_epoch: WriterEpoch,
    catalog: C,
    store: S,
    permit: CatalogWritePermit,
}

impl<C, S> StorageDomain<C, S>
where
    C: SystemCatalog,
    S: PageStore,
{
    pub fn new(id: StorageDomainId, writer_epoch: WriterEpoch, catalog: C, store: S) -> Self {
        Self {
            id,
            generation: 0,
            writer_epoch,
            catalog,
            store,
            permit: CatalogWritePermit { _private: () },
        }
    }

    /// Opens the latest durable generation and restores its generated catalog.
    pub fn open(
        id: StorageDomainId,
        writer_epoch: WriterEpoch,
        catalog: C,
        store: S,
    ) -> Result<Self, DomainError<S::Error>> {
        let permit = CatalogWritePermit { _private: () };
        let generation = match store.load_head(id).map_err(DomainError::Store)? {
            None => 0,
            Some(head) => {
                if head.domain != id {
                    return Err(DomainError::WrongDomain);
                }
                if head.catalog.offset != 0 || head.parent.checked_add(1) != Some(head.generation) {
                    return Err(DomainError::InvalidStoreResponse);
                }
                let checkpoint = store.load_catalog(&head).map_err(DomainError::Store)?;
                let prepared = catalog
                    .prepare_restore(&permit, &checkpoint)
                    .map_err(DomainError::Catalog)?;
                catalog.publish(&permit, prepared);
                head.generation
            }
        };
        Ok(Self {
            id,
            generation,
            writer_epoch,
            catalog,
            store,
            permit,
        })
    }

    pub fn id(&self) -> StorageDomainId {
        self.id
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn catalog(&self) -> &C {
        &self.catalog
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn begin_generation(&self) -> Result<GenerationBuilder, DomainError<S::Error>> {
        let id = self
            .generation
            .checked_add(1)
            .ok_or(DomainError::InvalidGeneration)?;
        Ok(GenerationBuilder {
            plan: GenerationPlan {
                domain: self.id,
                id,
                parent: self.generation,
                writer_epoch: self.writer_epoch,
                pages: Vec::new(),
                catalog_delta: Vec::new(),
            },
        })
    }

    pub fn commit_generation(
        &mut self,
        mut plan: GenerationPlan,
    ) -> Result<CommittedGeneration, DomainError<S::Error>> {
        if plan.parent != self.generation {
            return Err(DomainError::StaleParent {
                expected: self.generation,
                found: plan.parent,
            });
        }
        if plan.domain != self.id || plan.pages.iter().any(|page| page.address.domain != self.id) {
            return Err(DomainError::WrongDomain);
        }
        if plan.writer_epoch != self.writer_epoch {
            return Err(DomainError::StaleWriter);
        }
        if plan.id
            != plan
                .parent
                .checked_add(1)
                .ok_or(DomainError::InvalidGeneration)?
        {
            return Err(DomainError::InvalidGeneration);
        }

        let mut seen_pages = BTreeSet::new();
        for page in &mut plan.pages {
            if let MutationKind::Put {
                image,
                live_rows,
                live_bytes,
            } = &mut page.kind
            {
                if image.is_empty() {
                    return Err(DomainError::Catalog(CatalogError::InvalidMutation));
                }
                if page.address.page_kind == PageKind::Data {
                    let facts = crate::inspect_data_page_image(image).map_err(DomainError::Page)?;
                    if facts.page_id != page.address.page_id
                        || facts.space_id != page.address.space_id
                    {
                        return Err(DomainError::Catalog(CatalogError::InvalidMutation));
                    }
                    let table = plan
                        .catalog_delta
                        .iter()
                        .find_map(|mutation| match mutation {
                            CatalogMutation::Upsert(CatalogRecord::Table(table))
                                if table.table_id == page.address.table_id =>
                            {
                                Some(table.clone())
                            }
                            _ => None,
                        })
                        .or_else(|| {
                            match self
                                .catalog
                                .record(&CatalogRecord::table_key(page.address.table_id))
                            {
                                Some(CatalogRecord::Table(table)) => Some(table),
                                _ => None,
                            }
                        })
                        .ok_or(DomainError::Catalog(CatalogError::InvalidMutation))?;
                    if image.len() != table.page_stride as usize
                        || (table.data_space_id.0 != 0
                            && table.data_space_id != page.address.space_id)
                    {
                        return Err(DomainError::Catalog(CatalogError::InvalidMutation));
                    }
                    *live_rows = facts.live_rows;
                    *live_bytes = facts.live_bytes;
                }
            }
            if !seen_pages.insert(CatalogRecord::page_key(page.address))
                || (!plan.catalog_delta.iter().any(|mutation| {
                    matches!(
                        mutation,
                        CatalogMutation::Upsert(CatalogRecord::Table(row))
                            if row.table_id == page.address.table_id
                    )
                }) && self
                    .catalog
                    .record(&CatalogRecord::table_key(page.address.table_id))
                    .is_none())
            {
                return Err(DomainError::Catalog(CatalogError::InvalidMutation));
            }
        }

        let put_count = plan
            .pages
            .iter()
            .filter(|page| matches!(&page.kind, MutationKind::Put { .. }))
            .count();
        let mut staged = self.store.stage(&plan).map_err(DomainError::Store)?;
        let staged_keys = staged
            .pages
            .iter()
            .map(|page| CatalogRecord::page_key(page.address))
            .collect::<BTreeSet<_>>();
        if staged.domain != plan.domain
            || staged.generation != plan.id
            || staged.parent != plan.parent
            || staged.writer_epoch != plan.writer_epoch
            || staged.pages.len() != put_count
            || staged_keys.len() != put_count
            || staged.pages.iter().any(|page| {
                !plan.pages.iter().any(|mutation| {
                    mutation.address == page.address
                        && matches!(&mutation.kind, MutationKind::Put { .. })
                })
            })
        {
            return Err(DomainError::InvalidStoreResponse);
        }
        let mutations = self.catalog_mutations(&plan, &staged)?;

        let prepared = self
            .catalog
            .prepare(&self.permit, &mutations)
            .map_err(DomainError::Catalog)?;
        self.store
            .stage_catalog(&mut staged, prepared.checkpoint())
            .map_err(DomainError::Store)?;
        if staged.catalog.is_none() {
            return Err(DomainError::MissingCatalogObject);
        }
        let expected_catalog = staged.catalog.clone();
        let committed = self.store.commit(staged).map_err(DomainError::Store)?;
        if committed.head.domain != plan.domain
            || committed.head.generation != plan.id
            || committed.head.parent != plan.parent
            || committed.head.writer_epoch != plan.writer_epoch
            || Some(&committed.head.catalog) != expected_catalog.as_ref()
        {
            return Err(DomainError::InvalidStoreResponse);
        }
        self.catalog.publish(&self.permit, prepared);
        self.generation = committed.head.generation;
        Ok(committed)
    }

    fn catalog_mutations(
        &self,
        plan: &GenerationPlan,
        staged: &StagedGeneration,
    ) -> Result<Vec<CatalogMutation>, DomainError<S::Error>> {
        let mut mutations = plan.catalog_delta.clone();
        let mut tables = BTreeMap::new();
        for mutation in &plan.catalog_delta {
            if let CatalogMutation::Upsert(CatalogRecord::Table(row)) = mutation {
                tables.insert(row.table_id, row.clone());
            }
        }
        let staged_pages = staged
            .pages
            .iter()
            .map(|page| (CatalogRecord::page_key(page.address), page))
            .collect::<BTreeMap<_, _>>();

        for mutation in &plan.pages {
            let key = CatalogRecord::page_key(mutation.address);
            let old = match self.catalog.record(&key) {
                Some(CatalogRecord::Page(row)) => Some(row),
                Some(_) => return Err(DomainError::Catalog(CatalogError::InvalidMutation)),
                None => None,
            };
            if !tables.contains_key(&mutation.address.table_id) {
                let Some(CatalogRecord::Table(table)) = self
                    .catalog
                    .record(&CatalogRecord::table_key(mutation.address.table_id))
                else {
                    return Err(DomainError::Catalog(CatalogError::InvalidMutation));
                };
                tables.insert(table.table_id, table);
            }
            let table = tables
                .get_mut(&mutation.address.table_id)
                .ok_or(DomainError::Catalog(CatalogError::InvalidMutation))?;
            if mutation.address.page_kind == PageKind::Data {
                if table.data_space_id.0 == 0 {
                    table.data_space_id = mutation.address.space_id;
                } else if table.data_space_id != mutation.address.space_id {
                    return Err(DomainError::Catalog(CatalogError::InvalidMutation));
                }
            }
            if let Some(old) = &old {
                apply_page_totals(table, old.page_kind, old.live_rows, old.live_bytes, false)
                    .map_err(DomainError::Catalog)?;
            }

            match mutation.kind {
                MutationKind::Delete => mutations.push(CatalogMutation::Delete(key)),
                MutationKind::Put { .. } => {
                    let page = staged_pages
                        .get(&key)
                        .ok_or(DomainError::Catalog(CatalogError::InvalidMutation))?;
                    let record = SystemPageRecord {
                        table_id: page.address.table_id,
                        space_id: page.address.space_id,
                        page_id: page.address.page_id,
                        page_kind: page.address.page_kind,
                        generation: plan.id,
                        object: page.object.object,
                        object_offset: page.object.offset,
                        encoded_length: page.object.encoded_length,
                        decoded_length: page.object.decoded_length,
                        checksum: page.object.checksum,
                        live_rows: page.live_rows,
                        live_bytes: page.live_bytes,
                    };
                    apply_page_totals(
                        table,
                        record.page_kind,
                        record.live_rows,
                        record.live_bytes,
                        true,
                    )
                    .map_err(DomainError::Catalog)?;
                    mutations.push(CatalogMutation::Upsert(CatalogRecord::Page(record)));
                }
            }
        }
        for table in tables.values_mut() {
            table.applied_generation = plan.id;
            table.durable_generation = plan.id;
            mutations.push(CatalogMutation::Upsert(CatalogRecord::Table(table.clone())));
        }
        Ok(mutations)
    }

    pub fn read_page(
        &self,
        address: PageAddress,
    ) -> Result<Option<Vec<u8>>, DomainError<S::Error>> {
        if address.domain != self.id {
            return Err(DomainError::WrongDomain);
        }
        let key = CatalogRecord::page_key(address);
        let Some(CatalogRecord::Page(page)) = self.catalog.record(&key) else {
            return Ok(None);
        };
        let page_ref = PageRef {
            address,
            object: ObjectRef {
                object: page.object,
                offset: page.object_offset,
                encoded_length: page.encoded_length,
                decoded_length: page.decoded_length,
                checksum: page.checksum,
            },
        };
        self.store
            .read_page(&page_ref)
            .map(Some)
            .map_err(DomainError::Store)
    }
}

fn apply_page_totals(
    table: &mut SystemTableRecord,
    kind: PageKind,
    live_rows: u32,
    live_bytes: u32,
    add: bool,
) -> Result<(), CatalogError> {
    let update = |value: &mut u64, delta: u64| -> Result<(), CatalogError> {
        *value = if add {
            value.checked_add(delta)
        } else {
            value.checked_sub(delta)
        }
        .ok_or(CatalogError::InvalidMutation)?;
        Ok(())
    };
    match kind {
        PageKind::Data => {
            update(&mut table.row_count, u64::from(live_rows))?;
            update(&mut table.live_row_bytes, u64::from(live_bytes))?;
            update(&mut table.allocated_data_pages, 1)?;
            if live_rows != 0 {
                update(&mut table.live_data_pages, 1)?;
            }
        }
        PageKind::PrimaryIndex => {
            update(&mut table.primary_index_entries, u64::from(live_rows))?;
        }
        PageKind::SecondaryIndex => {
            update(&mut table.secondary_index_entries, u64::from(live_rows))?;
        }
        PageKind::Metadata => {}
    }
    Ok(())
}

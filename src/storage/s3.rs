//! S3-compatible page-segment store for a [`StorageDomain`](super::StorageDomain).
//!
//! The bootstrap object is the only fixed object. Page images and the generated
//! WorkTable catalog checkpoint are immutable, content-addressed objects. A
//! conditional bootstrap PUT publishes the generation after both are durable.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt::{Display, Formatter, Write as _};
use core::time::Duration;
use std::collections::BTreeSet;
use std::io::Read as _;
use std::sync::Mutex;

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use ureq::{Agent, Error as UreqError, Response};
use url::Url;

use super::{
    CommittedGeneration, GenerationPlan, Head, MutationKind, ObjectRef, PageRef, PageStore,
    StagedGeneration, StagedPage, StorageDomainId,
};

const HEAD_MAGIC: &[u8; 8] = b"DBS3H001";
const CATALOG_MAGIC: &[u8; 8] = b"DBCAT001";
const HEAD_FILE: &str = "head.v1";
const SEGMENT_TARGET: usize = 4 * 1024 * 1024;
const CATALOG_CHUNK_SIZE: usize = crate::PAGE_SIZE;
const SIGNED_URL_LIFETIME: Duration = Duration::from_secs(3600);

type StoredObject = (Vec<u8>, Option<String>);

#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket_name: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub region: String,
    pub prefix: Option<String>,
    pub virtual_host_style: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3StoreError {
    InvalidConfig,
    Transport,
    Status(u16),
    Conflict,
    MissingObject,
    CorruptObject,
    InvalidHead,
    LengthOverflow,
}

impl Display for S3StoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidConfig => write!(formatter, "invalid S3 page-store configuration"),
            Self::Transport => write!(formatter, "S3 page-store transport failed"),
            Self::Status(status) => write!(formatter, "S3 page-store returned HTTP {status}"),
            Self::Conflict => write!(formatter, "S3 bootstrap generation changed concurrently"),
            Self::MissingObject => write!(formatter, "S3 generation references a missing object"),
            Self::CorruptObject => {
                write!(formatter, "S3 object failed length or checksum validation")
            }
            Self::InvalidHead => write!(formatter, "S3 bootstrap head is invalid"),
            Self::LengthOverflow => {
                write!(formatter, "S3 page or segment length is not representable")
            }
        }
    }
}

impl core::error::Error for S3StoreError {}

pub struct S3PageStore {
    config: S3Config,
    bucket: Bucket,
    credentials: Credentials,
    client: Agent,
    known_objects: Mutex<BTreeSet<String>>,
}

impl S3PageStore {
    pub fn new(config: S3Config) -> Result<Self, S3StoreError> {
        let endpoint: Url = config
            .endpoint
            .parse()
            .map_err(|_| S3StoreError::InvalidConfig)?;
        let style = if config.virtual_host_style {
            UrlStyle::VirtualHost
        } else {
            UrlStyle::Path
        };
        let bucket = Bucket::new(
            endpoint,
            style,
            config.bucket_name.clone(),
            config.region.clone(),
        )
        .map_err(|_| S3StoreError::InvalidConfig)?;
        let credentials = match &config.session_token {
            Some(token) => Credentials::new_with_token(
                config.access_key.clone(),
                config.secret_key.clone(),
                token.clone(),
            ),
            None => Credentials::new(&config.access_key, &config.secret_key),
        };
        let client = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(120))
            .build();
        Ok(Self {
            config,
            bucket,
            credentials,
            client,
            known_objects: Mutex::new(BTreeSet::new()),
        })
    }

    fn object_key(&self, domain: StorageDomainId, suffix: &str) -> String {
        let mut domain_hex = String::with_capacity(32);
        write_hex(&mut domain_hex, &domain.0);
        let prefix = self
            .config
            .prefix
            .as_deref()
            .unwrap_or("")
            .trim_matches('/');
        if prefix.is_empty() {
            format!("{domain_hex}/{suffix}")
        } else {
            format!("{prefix}/{domain_hex}/{suffix}")
        }
    }

    fn segment_key(&self, domain: StorageDomainId, object: &[u8; 32]) -> String {
        let mut hash = String::with_capacity(64);
        write_hex(&mut hash, object);
        self.object_key(domain, &format!("segments/{hash}"))
    }

    fn catalog_key(&self, domain: StorageDomainId, object: &[u8; 32]) -> String {
        let mut hash = String::with_capacity(64);
        write_hex(&mut hash, object);
        self.object_key(domain, &format!("catalog/{hash}"))
    }

    fn catalog_chunk_key(&self, domain: StorageDomainId, object: &[u8; 32]) -> String {
        let mut hash = String::with_capacity(64);
        write_hex(&mut hash, object);
        self.object_key(domain, &format!("catalog-pages/{hash}"))
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>, S3StoreError> {
        let url = self
            .bucket
            .get_object(Some(&self.credentials), key)
            .sign(SIGNED_URL_LIFETIME);
        let response = match self.client.get(url.as_str()).call() {
            Ok(response) => response,
            Err(UreqError::Status(404, _)) => return Ok(None),
            Err(error) => return Err(map_ureq(error)),
        };
        let etag = response.header("etag").map(ToString::to_string);
        let mut bytes = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut bytes)
            .map_err(|_| S3StoreError::Transport)?;
        self.known_objects
            .lock()
            .map_err(|_| S3StoreError::Transport)?
            .insert(key.to_string());
        Ok(Some((bytes, etag)))
    }

    fn get_range(&self, key: &str, offset: u64, length: u32) -> Result<Vec<u8>, S3StoreError> {
        let end = offset
            .checked_add(u64::from(length))
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3StoreError::LengthOverflow)?;
        let url = self
            .bucket
            .get_object(Some(&self.credentials), key)
            .sign(SIGNED_URL_LIFETIME);
        let response = self
            .client
            .get(url.as_str())
            .set("range", &format!("bytes={offset}-{end}"))
            .call()
            .map_err(map_ureq)?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut bytes)
            .map_err(|_| S3StoreError::Transport)?;
        Ok(bytes)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<Response, S3StoreError> {
        let url = self
            .bucket
            .put_object(Some(&self.credentials), key)
            .sign(SIGNED_URL_LIFETIME);
        self.client
            .put(url.as_str())
            .send_bytes(bytes)
            .map_err(map_ureq)
    }

    fn put_verified(&self, key: &str, bytes: &[u8]) -> Result<(), S3StoreError> {
        if self
            .known_objects
            .lock()
            .map_err(|_| S3StoreError::Transport)?
            .contains(key)
        {
            return Ok(());
        }
        let result = match self.put(key, bytes) {
            Ok(_) => Ok(()),
            Err(error) => match self.get(key)? {
                Some((stored, _)) if stored == bytes => Ok(()),
                _ => Err(error),
            },
        };
        if result.is_ok() {
            self.known_objects
                .lock()
                .map_err(|_| S3StoreError::Transport)?
                .insert(key.to_string());
        }
        result
    }

    fn put_head(
        &self,
        domain: StorageDomainId,
        bytes: &[u8],
        current_etag: Option<&str>,
    ) -> Result<(), S3StoreError> {
        let key = self.object_key(domain, HEAD_FILE);
        let url = self
            .bucket
            .put_object(Some(&self.credentials), &key)
            .sign(SIGNED_URL_LIFETIME);
        let request = self.client.put(url.as_str());
        let request = match current_etag {
            Some(etag) => request.set("if-match", etag),
            None => request.set("if-none-match", "*"),
        };
        match request.send_bytes(bytes) {
            Ok(_) => Ok(()),
            Err(UreqError::Status(412, _)) => Err(S3StoreError::Conflict),
            Err(error) => match self.get(&key)? {
                Some((stored, _)) if stored == bytes => Ok(()),
                _ => Err(map_ureq(error)),
            },
        }
    }

    fn current_head(
        &self,
        domain: StorageDomainId,
    ) -> Result<Option<(Head, Option<String>)>, S3StoreError> {
        let key = self.object_key(domain, HEAD_FILE);
        let Some((bytes, etag)) = self.get(&key)? else {
            return Ok(None);
        };
        Ok(Some((decode_head(&bytes)?, etag)))
    }
}

impl PageStore for S3PageStore {
    type Error = S3StoreError;

    fn load_head(&self, domain: StorageDomainId) -> Result<Option<Head>, Self::Error> {
        self.current_head(domain)
            .map(|head| head.map(|(head, _)| head))
    }

    fn load_catalog(&self, head: &Head) -> Result<Vec<u8>, Self::Error> {
        if head.catalog.offset != 0 {
            return Err(S3StoreError::InvalidHead);
        }
        let key = self.catalog_key(head.domain, &head.catalog.object);
        let (manifest, _) = self.get(&key)?.ok_or(S3StoreError::MissingObject)?;
        if manifest.len() != head.catalog.encoded_length as usize
            || blake3::hash(&manifest).as_bytes() != &head.catalog.object
        {
            return Err(S3StoreError::CorruptObject);
        }
        let catalog = decode_catalog_manifest(&manifest, |object| {
            let key = self.catalog_chunk_key(head.domain, object);
            self.get(&key)?
                .map(|(bytes, _)| bytes)
                .ok_or(S3StoreError::MissingObject)
        })?;
        if catalog.len() != head.catalog.decoded_length as usize
            || blake3::hash(&catalog).as_bytes() != &head.catalog.checksum
        {
            return Err(S3StoreError::CorruptObject);
        }
        Ok(catalog)
    }

    fn read_page(&self, page: &PageRef) -> Result<Vec<u8>, Self::Error> {
        let key = self.segment_key(page.address.domain, &page.object.object);
        let bytes = self.get_range(&key, page.object.offset, page.object.encoded_length)?;
        if bytes.len() != page.object.decoded_length as usize
            || blake3::hash(&bytes).as_bytes() != &page.object.checksum
        {
            return Err(S3StoreError::CorruptObject);
        }
        Ok(bytes)
    }

    fn stage(&self, plan: &GenerationPlan) -> Result<StagedGeneration, Self::Error> {
        let domain = plan.domain;
        if plan.pages.iter().any(|page| page.address.domain != domain) {
            return Err(S3StoreError::InvalidHead);
        }

        let mut puts = plan
            .pages
            .iter()
            .filter_map(|page| match &page.kind {
                MutationKind::Put {
                    image,
                    live_rows,
                    live_bytes,
                } if !image.is_empty() => {
                    Some((page.address, image.as_slice(), *live_rows, *live_bytes))
                }
                MutationKind::Put { .. } => None,
                MutationKind::Delete => None,
            })
            .collect::<Vec<_>>();
        puts.sort_by_key(|(address, _, _, _)| {
            (
                address.table_id.0,
                address.space_id.0,
                address.page_kind as u8,
                usize::from(address.page_id),
            )
        });
        if puts.len()
            != plan
                .pages
                .iter()
                .filter(|page| matches!(&page.kind, MutationKind::Put { .. }))
                .count()
        {
            return Err(S3StoreError::LengthOverflow);
        }

        let mut pages = Vec::with_capacity(puts.len());
        let mut at = 0;
        while at < puts.len() {
            let first = at;
            let mut length = puts[at].1.len();
            at += 1;
            while at < puts.len()
                && adjacent(puts[at - 1].0, puts[at].0)
                && length.saturating_add(puts[at].1.len()) <= SEGMENT_TARGET
            {
                length += puts[at].1.len();
                at += 1;
            }

            let mut segment = Vec::with_capacity(length);
            for (_, image, _, _) in &puts[first..at] {
                segment.extend_from_slice(image);
            }
            let object = *blake3::hash(&segment).as_bytes();
            let key = self.segment_key(domain, &object);
            self.put_verified(&key, &segment)?;

            let mut offset = 0_u64;
            for (address, image, live_rows, live_bytes) in &puts[first..at] {
                let encoded_length =
                    u32::try_from(image.len()).map_err(|_| S3StoreError::LengthOverflow)?;
                pages.push(StagedPage {
                    address: *address,
                    object: ObjectRef {
                        object,
                        offset,
                        encoded_length,
                        decoded_length: encoded_length,
                        checksum: *blake3::hash(image).as_bytes(),
                    },
                    live_rows: *live_rows,
                    live_bytes: *live_bytes,
                });
                offset = offset
                    .checked_add(u64::from(encoded_length))
                    .ok_or(S3StoreError::LengthOverflow)?;
            }
        }

        Ok(StagedGeneration {
            domain,
            generation: plan.id,
            parent: plan.parent,
            writer_epoch: plan.writer_epoch,
            pages,
            catalog: None,
        })
    }

    fn stage_catalog(
        &self,
        staged: &mut StagedGeneration,
        checkpoint: &[u8],
    ) -> Result<(), Self::Error> {
        let mut chunks = Vec::new();
        for bytes in checkpoint.chunks(CATALOG_CHUNK_SIZE) {
            let object = *blake3::hash(bytes).as_bytes();
            let length = u32::try_from(bytes.len()).map_err(|_| S3StoreError::LengthOverflow)?;
            let key = self.catalog_chunk_key(staged.domain, &object);
            self.put_verified(&key, bytes)?;
            chunks.push((object, length));
        }
        let manifest = encode_catalog_manifest(checkpoint, &chunks)?;
        let object = *blake3::hash(&manifest).as_bytes();
        let key = self.catalog_key(staged.domain, &object);
        self.put_verified(&key, &manifest)?;
        let encoded_length =
            u32::try_from(manifest.len()).map_err(|_| S3StoreError::LengthOverflow)?;
        let decoded_length =
            u32::try_from(checkpoint.len()).map_err(|_| S3StoreError::LengthOverflow)?;
        staged.catalog = Some(ObjectRef {
            object,
            offset: 0,
            encoded_length,
            decoded_length,
            checksum: *blake3::hash(checkpoint).as_bytes(),
        });
        Ok(())
    }

    fn commit(&self, staged: StagedGeneration) -> Result<CommittedGeneration, Self::Error> {
        let catalog = staged.catalog.ok_or(S3StoreError::InvalidHead)?;
        let current = self.current_head(staged.domain)?;
        match &current {
            Some((head, _)) if head.generation != staged.parent => {
                return Err(S3StoreError::Conflict)
            }
            None if staged.parent != 0 => return Err(S3StoreError::Conflict),
            _ => {}
        }
        let head = Head {
            domain: staged.domain,
            generation: staged.generation,
            parent: staged.parent,
            writer_epoch: staged.writer_epoch,
            catalog,
        };
        let bytes = encode_head(&head);
        self.put_head(
            staged.domain,
            &bytes,
            current.as_ref().and_then(|(_, etag)| etag.as_deref()),
        )?;
        Ok(CommittedGeneration { head })
    }
}

fn adjacent(left: super::PageAddress, right: super::PageAddress) -> bool {
    left.domain == right.domain
        && left.table_id == right.table_id
        && left.space_id == right.space_id
        && left.page_kind == right.page_kind
        && usize::from(left.page_id).checked_add(1) == Some(usize::from(right.page_id))
}

fn encode_catalog_manifest(
    checkpoint: &[u8],
    chunks: &[([u8; 32], u32)],
) -> Result<Vec<u8>, S3StoreError> {
    let count = u32::try_from(chunks.len()).map_err(|_| S3StoreError::LengthOverflow)?;
    let length = u64::try_from(checkpoint.len()).map_err(|_| S3StoreError::LengthOverflow)?;
    let mut bytes = Vec::with_capacity(20 + chunks.len() * 36 + 32);
    bytes.extend_from_slice(CATALOG_MAGIC);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for (object, chunk_length) in chunks {
        bytes.extend_from_slice(object);
        bytes.extend_from_slice(&chunk_length.to_le_bytes());
    }
    bytes.extend_from_slice(blake3::hash(checkpoint).as_bytes());
    Ok(bytes)
}

fn decode_catalog_manifest<F>(manifest: &[u8], mut load: F) -> Result<Vec<u8>, S3StoreError>
where
    F: FnMut(&[u8; 32]) -> Result<Vec<u8>, S3StoreError>,
{
    if manifest.len() < 52 || manifest.get(..8) != Some(CATALOG_MAGIC) {
        return Err(S3StoreError::CorruptObject);
    }
    let length = read_catalog_u64(manifest, 8)?;
    let count = read_catalog_u32(manifest, 16)? as usize;
    let entries_length = count
        .checked_mul(36)
        .and_then(|value| value.checked_add(20))
        .ok_or(S3StoreError::LengthOverflow)?;
    let expected_length = entries_length
        .checked_add(32)
        .ok_or(S3StoreError::LengthOverflow)?;
    if manifest.len() != expected_length {
        return Err(S3StoreError::CorruptObject);
    }

    let mut checkpoint =
        Vec::with_capacity(usize::try_from(length).map_err(|_| S3StoreError::LengthOverflow)?);
    for index in 0..count {
        let at = 20 + index * 36;
        let mut object = [0; 32];
        object.copy_from_slice(&manifest[at..at + 32]);
        let chunk_length = read_catalog_u32(manifest, at + 32)? as usize;
        if chunk_length == 0 || chunk_length > CATALOG_CHUNK_SIZE {
            return Err(S3StoreError::CorruptObject);
        }
        let chunk = load(&object)?;
        if chunk.len() != chunk_length || blake3::hash(&chunk).as_bytes() != &object {
            return Err(S3StoreError::CorruptObject);
        }
        checkpoint.extend_from_slice(&chunk);
    }
    let checksum = manifest
        .get(entries_length..expected_length)
        .ok_or(S3StoreError::CorruptObject)?;
    if u64::try_from(checkpoint.len()) != Ok(length)
        || blake3::hash(&checkpoint).as_bytes() != checksum
    {
        return Err(S3StoreError::CorruptObject);
    }
    Ok(checkpoint)
}

fn read_catalog_u32(bytes: &[u8], at: usize) -> Result<u32, S3StoreError> {
    let value = bytes
        .get(at..at + 4)
        .ok_or(S3StoreError::CorruptObject)?
        .try_into()
        .map_err(|_| S3StoreError::CorruptObject)?;
    Ok(u32::from_le_bytes(value))
}

fn read_catalog_u64(bytes: &[u8], at: usize) -> Result<u64, S3StoreError> {
    let value = bytes
        .get(at..at + 8)
        .ok_or(S3StoreError::CorruptObject)?
        .try_into()
        .map_err(|_| S3StoreError::CorruptObject)?;
    Ok(u64::from_le_bytes(value))
}

fn encode_head(head: &Head) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(160);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&head.domain.0);
    bytes.extend_from_slice(&head.generation.to_le_bytes());
    bytes.extend_from_slice(&head.parent.to_le_bytes());
    bytes.extend_from_slice(&head.writer_epoch.to_le_bytes());
    bytes.extend_from_slice(&head.catalog.object);
    bytes.extend_from_slice(&head.catalog.offset.to_le_bytes());
    bytes.extend_from_slice(&head.catalog.encoded_length.to_le_bytes());
    bytes.extend_from_slice(&head.catalog.decoded_length.to_le_bytes());
    bytes.extend_from_slice(&head.catalog.checksum);
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    bytes
}

fn decode_head(bytes: &[u8]) -> Result<Head, S3StoreError> {
    const PAYLOAD: usize = 128;
    if bytes.len() != PAYLOAD + 32 || &bytes[..8] != HEAD_MAGIC {
        return Err(S3StoreError::InvalidHead);
    }
    if blake3::hash(&bytes[..PAYLOAD]).as_bytes() != &bytes[PAYLOAD..] {
        return Err(S3StoreError::InvalidHead);
    }
    let mut domain = [0; 16];
    domain.copy_from_slice(&bytes[8..24]);
    let generation = read_u64(bytes, 24)?;
    let parent = read_u64(bytes, 32)?;
    let writer_epoch = read_u64(bytes, 40)?;
    let mut object = [0; 32];
    object.copy_from_slice(&bytes[48..80]);
    let offset = read_u64(bytes, 80)?;
    let encoded_length = read_u32(bytes, 88)?;
    let decoded_length = read_u32(bytes, 92)?;
    let mut checksum = [0; 32];
    checksum.copy_from_slice(&bytes[96..128]);
    Ok(Head {
        domain: StorageDomainId(domain),
        generation,
        parent,
        writer_epoch,
        catalog: ObjectRef {
            object,
            offset,
            encoded_length,
            decoded_length,
            checksum,
        },
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, S3StoreError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or(S3StoreError::InvalidHead)?
        .try_into()
        .map_err(|_| S3StoreError::InvalidHead)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, S3StoreError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or(S3StoreError::InvalidHead)?
        .try_into()
        .map_err(|_| S3StoreError::InvalidHead)?;
    Ok(u64::from_le_bytes(value))
}

fn write_hex(output: &mut String, bytes: &[u8]) {
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
}

fn map_ureq(error: UreqError) -> S3StoreError {
    match error {
        UreqError::Status(status, _) => S3StoreError::Status(status),
        UreqError::Transport(_) => S3StoreError::Transport,
    }
}

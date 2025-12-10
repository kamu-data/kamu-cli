// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_molecule_domain::{
    MoleculeReadVersionedFileEntryError,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeVersionedFileContentProvider,
    MoleculeVersionedFileContentProviderError,
};

use crate::prelude::*;
use crate::queries::VersionedFileContentDownload;
use crate::queries::molecule::v2::{
    MoleculeAccessLevel,
    MoleculeCategory,
    MoleculeEncryptionMetadata,
    MoleculeTag,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeVersionedFile
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFile<'a> {
    data_room_entry: &'a kamu_molecule_domain::MoleculeDataRoomEntry,
}

impl<'a> MoleculeVersionedFile<'a> {
    pub fn new(
        data_room_entry: &'a kamu_molecule_domain::MoleculeDataRoomEntry,
    ) -> MoleculeVersionedFile<'a> {
        MoleculeVersionedFile { data_room_entry }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFile<'_> {
    pub async fn latest(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<MoleculeVersionedFileEntry<'_>>> {
        // TODO: detect that data room entry is also the latest one, and if so,
        // rely on denormalized info

        // Read the latest versioned file entry
        let maybe_versioned_file_entry =
            try_read_versioned_file_entry(ctx, &self.data_room_entry.reference, None, None).await?;

        if let Some(versioned_file_entry) = maybe_versioned_file_entry {
            Ok(Some(MoleculeVersionedFileEntry::new_prefetched(
                &self.data_room_entry.reference,
                versioned_file_entry,
            )))
        } else {
            tracing::warn!(
                versioned_file_dataset_id = %self.data_room_entry.reference,
                "Versioned file has no versions yet",
            );
            Ok(None)
        }
    }

    // TODO: consult before publishing this API versientry
    #[graphql(skip)]
    #[expect(unused)]
    #[expect(clippy::unused_async)]
    pub async fn matching(&self) -> MoleculeVersionedFileEntry<'_> {
        MoleculeVersionedFileEntry::new_matching_data_room_entry(self.data_room_entry)
    }

    pub async fn as_of(
        &self,
        ctx: &Context<'_>,
        block_hash: Multihash<'static>,
    ) -> Result<Option<MoleculeVersionedFileEntry<'_>>> {
        let block_hash: odf::Multihash = block_hash.into();

        let maybe_versioned_file_entry = try_read_versioned_file_entry(
            ctx,
            &self.data_room_entry.reference,
            None,
            Some(block_hash.clone()),
        )
        .await?;

        if let Some(versioned_file_entry) = maybe_versioned_file_entry {
            Ok(Some(MoleculeVersionedFileEntry::new_prefetched(
                &self.data_room_entry.reference,
                versioned_file_entry,
            )))
        } else {
            tracing::warn!(
                versioned_file_dataset_id = %self.data_room_entry.reference,
                block_hash = %block_hash,
                "No matching versioned file entry found for block hash",
            );
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum MoleculeVersionedFileEntrySource<'a> {
    MatchingDataRoomEntry {
        data_room_entry: &'a kamu_molecule_domain::MoleculeDataRoomEntry,
        versioned_file_entry:
            tokio::sync::OnceCell<Option<kamu_molecule_domain::MoleculeVersionedFileEntry>>,
    },

    FullyPrefetched {
        versioned_file_dataset_id: &'a odf::DatasetID,
        versioned_file_entry: kamu_molecule_domain::MoleculeVersionedFileEntry,
    },
}

impl<'a> MoleculeVersionedFileEntrySource<'a> {
    fn new_matching_data_room_entry(
        data_room_entry: &'a kamu_molecule_domain::MoleculeDataRoomEntry,
    ) -> Self {
        Self::MatchingDataRoomEntry {
            data_room_entry,
            versioned_file_entry: tokio::sync::OnceCell::new(),
        }
    }

    fn new_prefetched(
        versioned_file_dataset_id: &'a odf::DatasetID,
        versioned_file_entry: kamu_molecule_domain::MoleculeVersionedFileEntry,
    ) -> Self {
        Self::FullyPrefetched {
            versioned_file_dataset_id,
            versioned_file_entry,
        }
    }

    fn dataset_id(&self) -> &odf::DatasetID {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => &data_room_entry.reference,

            Self::FullyPrefetched {
                versioned_file_dataset_id,
                ..
            } => versioned_file_dataset_id,
        }
    }

    fn system_time(&self) -> DateTime<Utc> {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } =>
            // Strictly speaking, this is not the system_time of the versioned file,
            // but of the data room entry pointing to it, but this is a cheap approximation
            {
                data_room_entry.system_time
            }

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.system_time,
        }
    }

    fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry.event_time,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.event_time,
        }
    }

    fn version(&self) -> u32 {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry.denormalized_latest_file_info.version,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.version,
        }
    }

    fn content_hash(&self) -> &odf::Multihash {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => &data_room_entry.denormalized_latest_file_info.content_hash,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => &versioned_file_entry.content_hash,
        }
    }

    fn content_length(&self) -> usize {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry.denormalized_latest_file_info.content_length,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.content_length,
        }
    }

    fn content_type(&self) -> &str {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry
                .denormalized_latest_file_info
                .content_type
                .0
                .as_str(),

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.content_type.0.as_str(),
        }
    }

    fn access_level(&self) -> &MoleculeAccessLevel {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => &data_room_entry.denormalized_latest_file_info.access_level,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => &versioned_file_entry.basic_info.access_level,
        }
    }

    fn change_by(&self) -> &str {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry
                .denormalized_latest_file_info
                .change_by
                .as_str(),

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.basic_info.change_by.as_str(),
        }
    }

    fn description(&self) -> Option<&str> {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => data_room_entry
                .denormalized_latest_file_info
                .description
                .as_deref(),

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => versioned_file_entry.basic_info.description.as_deref(),
        }
    }

    fn categories(&self) -> &[MoleculeCategory] {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => &data_room_entry.denormalized_latest_file_info.categories,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => &versioned_file_entry.basic_info.categories,
        }
    }

    fn tags(&self) -> &[MoleculeTag] {
        match self {
            Self::MatchingDataRoomEntry {
                data_room_entry, ..
            } => &data_room_entry.denormalized_latest_file_info.tags,

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => &versioned_file_entry.basic_info.tags,
        }
    }

    async fn content_text(&self, ctx: &Context<'_>) -> Result<Option<&str>> {
        // Content text is stored only in the versioned file entry
        let maybe_versioned_file_entry = self.__internal_versioned_file_entry(ctx).await?;

        Ok(
            maybe_versioned_file_entry
                .and_then(|entry| entry.detailed_info.content_text.as_deref()),
        )
    }

    async fn encryption_metadata(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<MoleculeEncryptionMetadata>> {
        // Encryption metadata is stored only in the versioned file entry
        let maybe_versioned_file_entry = self.__internal_versioned_file_entry(ctx).await?;

        Ok(maybe_versioned_file_entry
            .as_ref()
            .and_then(|entry| entry.detailed_info.encryption_metadata.as_ref())
            .map(|metadata_record| metadata_record.as_entity().into()))
    }

    async fn __internal_versioned_file_entry(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<&kamu_molecule_domain::MoleculeVersionedFileEntry>> {
        Ok(match self {
            Self::MatchingDataRoomEntry {
                versioned_file_entry,
                data_room_entry,
            } => versioned_file_entry
                .get_or_try_init(|| {
                    // Read the record that exactly matches the data room entry
                    try_read_versioned_file_entry(
                        ctx,
                        &data_room_entry.reference,
                        Some(data_room_entry.denormalized_latest_file_info.version),
                        None,
                    )
                })
                .await?
                .as_ref(),

            Self::FullyPrefetched {
                versioned_file_entry,
                ..
            } => Some(versioned_file_entry),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileEntry<'a> {
    source: MoleculeVersionedFileEntrySource<'a>,
}

impl<'a> MoleculeVersionedFileEntry<'a> {
    pub fn new_matching_data_room_entry(
        data_room_entry: &'a kamu_molecule_domain::MoleculeDataRoomEntry,
    ) -> Self {
        Self {
            source: MoleculeVersionedFileEntrySource::new_matching_data_room_entry(data_room_entry),
        }
    }

    pub fn new_prefetched(
        versioned_file_dataset_id: &'a odf::DatasetID,
        versioned_file_entry: kamu_molecule_domain::MoleculeVersionedFileEntry,
    ) -> Self {
        Self {
            source: MoleculeVersionedFileEntrySource::new_prefetched(
                versioned_file_dataset_id,
                versioned_file_entry,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileEntry<'_> {
    async fn system_time(&self) -> DateTime<Utc> {
        self.source.system_time()
    }

    async fn event_time(&self) -> DateTime<Utc> {
        self.source.event_time()
    }

    async fn version(&self) -> u32 {
        self.source.version()
    }

    async fn content_hash(&self) -> Multihash<'_> {
        Multihash::from(self.source.content_hash())
    }

    async fn content_length(&self) -> usize {
        self.source.content_length()
    }

    async fn content_type(&self) -> &str {
        self.source.content_type()
    }

    async fn access_level(&self) -> &MoleculeAccessLevel {
        self.source.access_level()
    }

    async fn change_by(&self) -> &str {
        self.source.change_by()
    }

    async fn description(&self) -> Option<&str> {
        self.source.description()
    }

    async fn categories(&self) -> &[MoleculeCategory] {
        self.source.categories()
    }

    async fn tags(&self) -> &[MoleculeTag] {
        self.source.tags()
    }

    async fn content_text(&self, ctx: &Context<'_>) -> Result<Option<&str>> {
        self.source.content_text(ctx).await
    }

    async fn encryption_metadata(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<MoleculeEncryptionMetadata>> {
        // Encryption metadata is stored only in the versioned file entry,
        // so we need to read it first time only
        self.source.encryption_metadata(ctx).await
    }

    /// Returns encoded content in-band. Should be used for small files only and
    /// will return an error if called on large data.
    pub async fn content(&self, ctx: &Context<'_>) -> Result<Base64Usnp> {
        let molecule_versioned_file_content_provider =
            from_catalog_n!(ctx, dyn MoleculeVersionedFileContentProvider);

        let content_bytes = molecule_versioned_file_content_provider
            .get_versioned_file_content(self.source.dataset_id(), self.source.content_hash())
            .await
            .map_err(|e| {
                use MoleculeVersionedFileContentProviderError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(Base64Usnp::from(content_bytes))
    }

    /// Returns a direct download URL
    async fn content_url(&self, ctx: &Context<'_>) -> Result<VersionedFileContentDownload> {
        let molecule_versioned_file_content_provider =
            from_catalog_n!(ctx, dyn MoleculeVersionedFileContentProvider);

        let download_data = molecule_versioned_file_content_provider
            .get_versioned_file_content_download_data(
                self.source.dataset_id(),
                self.source.content_hash(),
            )
            .await
            .map_err(|e| {
                use MoleculeVersionedFileContentProviderError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(VersionedFileContentDownload::from(download_data))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn try_read_versioned_file_entry(
    ctx: &Context<'_>,
    reference: &odf::DatasetID,
    as_of_version: Option<kamu_datasets::FileVersion>,
    as_of_block_hash: Option<odf::Multihash>,
) -> Result<Option<kamu_molecule_domain::MoleculeVersionedFileEntry>> {
    let read_versioned_file_entry_uc =
        from_catalog_n!(ctx, dyn MoleculeReadVersionedFileEntryUseCase);

    let maybe_versioned_file_entry = read_versioned_file_entry_uc
        .execute(reference, as_of_version, as_of_block_hash)
        .await
        .map_err(|e| {
            use MoleculeReadVersionedFileEntryError as E;
            match e {
                E::Access(e) => GqlError::Access(e),
                E::Internal(e) => e.int_err().into(),
            }
        })?;

    Ok(maybe_versioned_file_entry)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

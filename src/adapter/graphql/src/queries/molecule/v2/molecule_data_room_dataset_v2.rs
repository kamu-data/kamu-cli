// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use url::Url;

use crate::prelude::*;
use crate::queries::molecule::v2::{
    EncryptionMetadata,
    MoleculeAccessLevelV2,
    MoleculeCategoryV2,
    MoleculeProjectV2,
    MoleculeTagV2,
};
use crate::queries::{Collection, Dataset, FileVersion, VersionedFile};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn entries(
        &self,
        _ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeDataRoomEntryV2Connection> {
        let _ = path_prefix;
        let _ = max_depth;
        let _ = page;
        let _ = per_page;

        // TODO: implement
        Ok(MoleculeDataRoomEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
    async fn entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<Option<MoleculeDataRoomEntryV2>> {
        let _ = path;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntryV2;

impl MoleculeDataRoomEntryV2 {
    // Extra columns
    pub const COLUMN_NAME_CHANGE_BY: &'static str = "molecule_change_by";
    pub const COLUMN_NAME_ACCESS_LEVEL: &'static str = "molecule_access_level";
    // Denormalized values from the latest file state
    pub const COLUMN_NAME_CONTENT_TYPE: &'static str = "content_type";
    pub const COLUMN_NAME_CONTENT_LENGTH: &'static str = "content_length";
    pub const COLUMN_NAME_CATEGORIES: &'static str = "categories";
    pub const COLUMN_NAME_TAGS: &'static str = "tags";
    pub const COLUMN_NAME_VERSION: &'static str = "version";

    pub fn dataset_snapshot(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        Collection::dataset_snapshot(
            alias,
            vec![
                // Extra columns
                ColumnInput::string(Self::COLUMN_NAME_ACCESS_LEVEL),
                ColumnInput::string(Self::COLUMN_NAME_CHANGE_BY),
                // Denormalized values from the latest file state
                ColumnInput::string(Self::COLUMN_NAME_CONTENT_TYPE),
                ColumnInput::int(Self::COLUMN_NAME_CONTENT_LENGTH),
                ColumnInput::string_array(Self::COLUMN_NAME_CATEGORIES),
                ColumnInput::string_array(Self::COLUMN_NAME_TAGS),
                // TODO: unsinged int?
                ColumnInput::int(Self::COLUMN_NAME_VERSION),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn build_extra_data_json_map(
        access_level: &MoleculeAccessLevelV2,
        change_by: &AccountID<'static>,
        content_type: &String,
        content_length: usize,
        categories: &Vec<MoleculeCategoryV2>,
        tags: &Vec<MoleculeTagV2>,
        version: u32,
    ) -> serde_json::Map<String, serde_json::Value> {
        let json_object = serde_json::json!({
            Self::COLUMN_NAME_ACCESS_LEVEL: access_level,
            Self::COLUMN_NAME_CHANGE_BY: change_by.to_string(),
            Self::COLUMN_NAME_CONTENT_TYPE: content_type,
            Self::COLUMN_NAME_CONTENT_LENGTH: content_length,
            Self::COLUMN_NAME_CATEGORIES: categories,
            Self::COLUMN_NAME_TAGS: tags,
            Self::COLUMN_NAME_VERSION: version,
        });

        let serde_json::Value::Object(json_map) = json_object else {
            unreachable!()
        };

        json_map
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntryV2 {
    #[expect(clippy::unused_async)]
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn path(&self, _ctx: &Context<'_>) -> Result<CollectionPath> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    #[graphql(name = "ref")]
    async fn reference(&self, _ctx: &Context<'_>) -> Result<DatasetID<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Access the linked core Dataset
    async fn as_dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Strongly typed [`MoleculeVersionedFileV2`] object
    async fn as_versioned_file(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileV2> {
        todo!()
    }
}

page_based_connection!(
    MoleculeDataRoomEntryV2,
    MoleculeDataRoomEntryV2Connection,
    MoleculeDataRoomEntryV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileV2;

impl MoleculeVersionedFileV2 {
    // Extra columns
    pub const COLUMN_NAME_ACCESS_LEVEL: &'static str = "molecule_access_level";
    pub const COLUMN_NAME_CHANGE_BY: &'static str = "molecule_change_by";
    // Extended metadata
    pub const COLUMN_NAME_DESCRIPTION: &'static str = "description";
    pub const COLUMN_NAME_CATEGORIES: &'static str = "categories";
    pub const COLUMN_NAME_TAGS: &'static str = "tags";
    // Semantic search
    pub const COLUMN_NAME_CONTENT_TEXT: &'static str = "content_text";
    // E2EE
    pub const COLUMN_NAME_ENCRYPTION_METADATA: &'static str = "encryption_metadata";

    pub fn dataset_snapshot(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        VersionedFile::dataset_snapshot(
            alias,
            vec![
                // Extra columns
                ColumnInput::string(Self::COLUMN_NAME_ACCESS_LEVEL),
                ColumnInput::string(Self::COLUMN_NAME_CHANGE_BY),
                ColumnInput::string(Self::COLUMN_NAME_DESCRIPTION),
                // Extended metadata
                ColumnInput::string_array(Self::COLUMN_NAME_CATEGORIES),
                ColumnInput::string_array(Self::COLUMN_NAME_TAGS),
                // Semantic search
                ColumnInput::string(Self::COLUMN_NAME_CONTENT_TEXT),
                // E2EE
                ColumnInput::string(Self::COLUMN_NAME_ENCRYPTION_METADATA),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn build_extra_data_json_map(
        access_level: &MoleculeAccessLevelV2,
        change_by: &AccountID<'static>,
        description: &String,
        categories: &Vec<MoleculeCategoryV2>,
        tags: &Vec<MoleculeTagV2>,
        content_text: &String,
        encryption_metadata: &Option<Json<EncryptionMetadata>>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let json_object = serde_json::json!({
            Self::COLUMN_NAME_ACCESS_LEVEL: access_level,
            Self::COLUMN_NAME_CHANGE_BY: change_by.to_string(),
            Self::COLUMN_NAME_DESCRIPTION: description,
            Self::COLUMN_NAME_CATEGORIES: categories,
            Self::COLUMN_NAME_TAGS: tags,
            Self::COLUMN_NAME_CONTENT_TEXT: content_text,
            Self::COLUMN_NAME_ENCRYPTION_METADATA: encryption_metadata,
        });

        let serde_json::Value::Object(json_map) = json_object else {
            unreachable!()
        };

        json_map
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileV2 {
    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn version(&self, _ctx: &Context<'_>) -> Result<FileVersion> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_hash(&self, _ctx: &Context<'_>) -> Result<Multihash<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_length(&self, _ctx: &Context<'_>) -> Result<usize> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    // TODO: typing
    async fn content_type(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevelV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<MoleculeCategoryV2>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<MoleculeTagV2>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_url(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileContentUrlV2> {
        todo!()
    }

    // todo encryptionMetadata
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: maybe just reuse from v1?
#[derive(SimpleObject)]
pub struct MoleculeVersionedFileContentUrlV2 {
    pub url: Url,
    pub headers: HashMap<String, String>,
    // TODO: typing
    pub method: String,
    pub expires_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

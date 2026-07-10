// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Display;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<SpecT, SpecTInput> From<resource::Resource<SpecT>> for resource::ResourceInput<SpecTInput>
where
    SpecT: Into<SpecTInput>,
{
    fn from(value: resource::Resource<SpecT>) -> Self {
        let resource::Resource {
            schema,
            headers,
            spec,
            status: _,
        } = value;
        Self {
            schema,
            headers: headers.into(),
            spec: spec.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceHeaders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<resource::ResourceHeaders> for resource::ResourceHeadersInput {
    fn from(value: resource::ResourceHeaders) -> Self {
        let resource::ResourceHeaders {
            id,
            name,
            account,
            labels,
            annotations,
            generation: _,
            created_at: _,
            updated_at: _,
            deleted_at: _,
        } = value;
        Self {
            id: Some(id),
            name,
            account: Some(account.into()),
            labels: Some(labels),
            annotations: Some(annotations),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for resource::ResourceRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((typ, account, name)) = Grammar::match_resource_ref(s) else {
            return Err(::multiformats::ParseError::<Self>::new(s));
        };

        Ok(Self {
            account: account.map(|s| AccountName::new_unchecked(s).into()),
            r#type: resource::TypeName::new_unchecked(typ).into(),
            id: None,
            did: None,
            name: Some(resource::ResourceName::new_unchecked(name)),
        })
    }
}

impl_parse_error!(resource::ResourceRef);
impl_try_from_str!(resource::ResourceRef);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<resource::ResourceHandle> for resource::ResourceRef {
    fn from(value: resource::ResourceHandle) -> Self {
        let resource::ResourceHandle {
            account,
            r#type,
            id,
            did,
            name,
        } = value;
        Self {
            account: Some(account.into()),
            r#type: r#type.into(),
            id: Some(id),
            did,
            name: Some(name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for auth::AccountRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((name, "")) = Grammar::match_account_name(s) else {
            return Err(::multiformats::ParseError::<Self>::new(s));
        };

        Ok(Self {
            id: None,
            did: None,
            name: Some(auth::AccountName::new_unchecked(name)),
        })
    }
}

impl_parse_error!(auth::AccountRef);
impl_try_from_str!(auth::AccountRef);

impl From<auth::AccountID> for auth::AccountRef {
    fn from(value: AccountID) -> Self {
        Self {
            id: None,
            did: Some(value),
            name: None,
        }
    }
}

impl From<auth::AccountName> for auth::AccountRef {
    fn from(value: AccountName) -> Self {
        Self {
            id: None,
            did: None,
            name: Some(value),
        }
    }
}

impl From<auth::AccountHandle> for auth::AccountRef {
    fn from(value: auth::AccountHandle) -> Self {
        let auth::AccountHandle { id, did, name } = value;
        Self {
            id: Some(id),
            did: Some(did),
            name: Some(name),
        }
    }
}

impl From<auth::AccountRef> for resource::ResourceRef {
    fn from(value: auth::AccountRef) -> Self {
        let auth::AccountRef { id, did, name } = value;
        Self {
            account: None,
            r#type: auth::Account::schema().clone().into(),
            id,
            did: did.map(Into::into),
            name: name.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<auth::AccountHandle> for resource::ResourceRef {
    fn from(value: auth::AccountHandle) -> Self {
        auth::AccountRef::from(value).into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Secret
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for config::Secret {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            value: s.into(),
            content_encoding: None,
        })
    }
}

impl_parse_error!(config::Secret);
impl_try_from_str!(config::Secret);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Variable
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for config::Variable {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { value: s.into() })
    }
}

impl_parse_error!(config::Variable);
impl_try_from_str!(config::Variable);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ValueRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for config::ValueRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(config::ValueRef);
impl_try_from_str!(config::ValueRef);

impl From<config::ValueRef> for resource::ResourceRef {
    fn from(value: config::ValueRef) -> Self {
        let config::ValueRef {
            account,
            r#type,
            id,
            name,
            path: _,
        } = value;
        Self {
            account,
            r#type,
            id,
            did: None,
            name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ValueHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<config::ValueHandle> for resource::ResourceRef {
    fn from(value: config::ValueHandle) -> Self {
        let config::ValueHandle {
            account,
            r#type,
            id,
            name,
            path: _,
        } = value;
        Self {
            account: Some(account.into()),
            r#type: r#type.into(),
            id: Some(id),
            did: None,
            name: Some(name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PersistentVolumeRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for storage::PersistentVolumeRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(storage::PersistentVolumeRef);
impl_try_from_str!(storage::PersistentVolumeRef);

impl From<storage::PersistentVolumeRef> for resource::ResourceRef {
    fn from(value: storage::PersistentVolumeRef) -> Self {
        let storage::PersistentVolumeRef { account, id, name } = value;
        Self {
            account,
            r#type: storage::PersistentVolume::schema().clone().into(),
            id,
            did: None,
            name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::OffsetInterval {
    #[allow(clippy::cast_possible_truncation)]
    pub fn len(&self) -> usize {
        (self.end - self.start + 1) as usize
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::AddData {
    /// Helper for determining the last record offset in the dataset
    pub fn last_offset(&self) -> Option<u64> {
        self.new_data
            .as_ref()
            .map(|d| d.offset_interval.end)
            .or(self.prev_offset)
    }

    pub fn is_empty(&self) -> bool {
        self.new_data.is_none()
            && self.new_checkpoint.is_none()
            && self.new_watermark.is_none()
            && self.new_source_state.is_none()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::ExecuteTransform {
    /// Helper for determining the last record offset in the dataset
    pub fn last_offset(&self) -> Option<u64> {
        self.new_data
            .as_ref()
            .map(|d| d.offset_interval.end)
            .or(self.prev_offset)
    }

    pub fn is_empty(&self) -> bool {
        self.new_data.is_none() && self.new_checkpoint.is_none() && self.new_watermark.is_none()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::ExecuteTransformInput {
    /// Helper for determining the input's last block hash included in the
    /// transaction
    pub fn last_block_hash(&self) -> Option<&Multihash> {
        self.new_block_hash
            .as_ref()
            .or(self.prev_block_hash.as_ref())
    }

    /// Helper for determining the input's last record offset included in the
    /// transaction
    pub fn last_offset(&self) -> Option<u64> {
        self.new_offset.or(self.prev_offset)
    }

    /// Helper for determining the number of records included in the transaction
    /// from this input
    pub fn num_records(&self) -> u64 {
        if let Some(new_offset) = self.new_offset {
            if let Some(prev_offset) = self.prev_offset {
                new_offset - prev_offset
            } else {
                new_offset + 1
            }
        } else {
            0
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::SetTransform {
    pub fn as_dataset_ref_alias_map(&self) -> HashMap<&dataset::DatasetRef, &String> {
        self.inputs.iter().fold(HashMap::new(), |mut acc, input| {
            if let Some(alias) = input.alias.as_ref() {
                acc.insert(&input.dataset_ref, alias);
            }
            acc
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::Transform {
    pub fn engine(&self) -> &str {
        match self {
            Self::Sql(v) => v.engine.as_str(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<dataset::SetVocab> for dataset::DatasetVocabulary {
    fn from(v: dataset::SetVocab) -> Self {
        Self {
            offset_column: v.offset_column,
            operation_type_column: v.operation_type_column,
            system_time_column: v.system_time_column,
            event_time_column: v.event_time_column,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl source::ReadStep {
    pub fn ddl_schema(&self) -> Option<&Vec<String>> {
        match self {
            Self::Csv(v) => v.ddl_schema.as_ref(),
            Self::Json(v) => v.ddl_schema.as_ref(),
            Self::NdJson(v) => v.ddl_schema.as_ref(),
            Self::GeoJson(v) => v.ddl_schema.as_ref(),
            Self::NdGeoJson(v) => v.ddl_schema.as_ref(),
            Self::EsriShapefile(v) => v.ddl_schema.as_ref(),
            Self::Parquet(v) => v.ddl_schema.as_ref(),
        }
    }

    pub fn schema(&self) -> Option<&data::DataSchema> {
        match self {
            Self::Csv(v) => v.schema.as_ref(),
            Self::Json(v) => v.schema.as_ref(),
            Self::NdJson(v) => v.schema.as_ref(),
            Self::GeoJson(v) => v.schema.as_ref(),
            Self::NdGeoJson(v) => v.schema.as_ref(),
            Self::EsriShapefile(v) => v.schema.as_ref(),
            Self::Parquet(v) => v.schema.as_ref(),
        }
    }

    pub fn set_schema(&mut self, schema: data::DataSchema) {
        match self {
            Self::Csv(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::Json(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::NdJson(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::GeoJson(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::NdGeoJson(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::EsriShapefile(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
            Self::Parquet(v) => {
                v.ddl_schema = None;
                v.schema = Some(schema);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::DatasetVocabulary {
    pub fn set_defaults(&mut self) -> &mut Self {
        self.offset_column
            .get_or_insert_with(|| Self::default_offset_column().into());
        self.operation_type_column
            .get_or_insert_with(|| Self::default_operation_type_column().into());
        self.system_time_column
            .get_or_insert_with(|| Self::default_system_time_column().into());
        self.event_time_column
            .get_or_insert_with(|| Self::default_event_time_column().into());
        self
    }

    pub fn with_defaults(mut self) -> Self {
        self.set_defaults();
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl source::MergeStrategy {
    pub fn primary_key(self) -> Option<Vec<String>> {
        match self {
            Self::Append(_a) => None,
            Self::Ledger(l) => Some(l.primary_key),
            Self::Snapshot(s) => Some(s.primary_key),
            Self::ChangelogStream(c) => Some(c.primary_key),
            Self::UpsertStream(u) => Some(u.primary_key),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Display for engine::RawQueryResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for engine::RawQueryResponseInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)?;
        if let Some(bt) = &self.backtrace {
            write!(f, "\n\n--- Engine Backtrace ---\n{bt}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Display for engine::TransformResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for engine::TransformResponseInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)?;
        if let Some(bt) = &self.backtrace {
            write!(f, "\n\n--- Engine Backtrace ---\n{bt}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::DataSlice {
    pub fn num_records(&self) -> u64 {
        self.offset_interval.end - self.offset_interval.start + 1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Normalized representation of [`SetDataSchema`] that uses new schema format
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetDataSchemaV2 {
    /// Defines the logical schema of the data files that follow this event.
    /// Will become a required field after migration.
    pub schema: data::DataSchema,
}

impl dataset::SetDataSchema {
    pub fn new(schema: data::DataSchema) -> Self {
        Self {
            raw_arrow_schema: None,
            schema: Some(schema),
        }
    }

    // Convert legacy schema into new schema
    #[cfg(feature = "arrow")]
    pub fn upgrade(self) -> SetDataSchemaV2 {
        if let Some(schema) = self.schema {
            SetDataSchemaV2 { schema }
        } else {
            let arrow_schema = self
                .schema_as_arrow(&data::ToArrowSettings::default())
                .unwrap();

            // SAFETY: Old version of the event was writing schemas after execution of our
            // engines which produce the subset of types that we know for certain are
            // compatible with ODF schema, so unwrapping is safe.
            let schema = data::DataSchema::new_from_arrow(&arrow_schema).unwrap();

            // NOTE: Previously Arrow schema was written as it appeared in the output
            // DataFrame. This included View type encodings. ODF schema makes a decision to
            // only store logical types, thus we strip all possible encodings.
            let schema = schema.strip_encoding();

            SetDataSchemaV2 { schema }
        }
    }

    #[cfg(feature = "arrow")]
    #[deprecated(
        note = "Legacy format is being phased out. All new events of this type must be written \
                with ODF schema. Arrow schema remains for compatibility with existing datasets, \
                but will be dropped in the upcoming versions when all datasets migrate to ODF \
                schema."
    )]
    pub fn new_legacy_raw_arrow(schema: &arrow::datatypes::Schema) -> Self {
        let mut encoder = arrow::ipc::convert::IpcSchemaEncoder::new();
        let (mut buf, head) = encoder.schema_to_fb(schema).collapse();
        buf.drain(0..head);
        Self {
            raw_arrow_schema: Some(buf),
            schema: None,
        }
    }

    #[cfg(feature = "arrow")]
    pub fn schema_as_arrow(
        &self,
        settings: &data::ToArrowSettings,
    ) -> Result<arrow::datatypes::Schema, SchemaAsArrowError> {
        if let Some(raw_arrow_schema) = &self.raw_arrow_schema {
            assert!(self.schema.is_none());

            let schema_proxy =
                flatbuffers::root::<arrow::ipc::r#gen::Schema::Schema>(raw_arrow_schema)
                    .map_err(crate::serde::Error::serde)?;
            let schema = arrow::ipc::convert::fb_to_schema(schema_proxy);
            Ok(schema)
        } else if let Some(schema) = &self.schema {
            Ok(schema.to_arrow(settings)?)
        } else {
            unreachable!("Neither raw or structured schema found")
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum SchemaAsArrowError {
    Serde(#[from] crate::serde::Error),
    Unsupported(#[from] crate::data::UnsupportedSchema),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl source::SourceState {
    pub const DEFAULT_SOURCE_NAME: &'static str = "default";
    pub const KIND_ETAG: &'static str = "odf/etag";
    pub const KIND_LAST_MODIFIED: &'static str = "odf/last-modified";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEventTypeFlags
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl dataset::MetadataEventTypeFlags {
    pub const DATA_BLOCK: Self =
        Self::from_bits_retain(Self::ADD_DATA.bits() | Self::EXECUTE_TRANSFORM.bits());

    pub const KEY_BLOCK: Self =
        Self::from_bits_retain(Self::all().difference(Self::DATA_BLOCK).bits());

    pub fn has_data_flags(&self) -> bool {
        !(*self & Self::DATA_BLOCK).is_empty()
    }

    pub fn has_key_block_flags(&self) -> bool {
        !(*self & Self::KEY_BLOCK).is_empty()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SecretSetSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<config::SecretSetSpec> for config::SecretSetSpecInput {
    fn from(v: config::SecretSetSpec) -> Self {
        let config::SecretSetSpec { secrets } = v;
        Self { secrets }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

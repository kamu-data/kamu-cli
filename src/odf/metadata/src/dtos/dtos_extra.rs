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

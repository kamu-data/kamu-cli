// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::derivable_impls)]

use std::fmt::Display;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AddData {
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

impl ExecuteTransform {
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

impl ExecuteTransformInput {
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
// Transform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Transform {
    pub fn engine(&self) -> &str {
        match self {
            Transform::Sql(v) => v.engine.as_str(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for SetVocab {
    fn default() -> Self {
        Self {
            offset_column: None,
            operation_type_column: None,
            system_time_column: None,
            event_time_column: None,
        }
    }
}

impl From<SetVocab> for DatasetVocabulary {
    fn from(v: SetVocab) -> Self {
        Self {
            offset_column: v
                .offset_column
                .unwrap_or_else(|| DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME.to_string()),
            operation_type_column: v.operation_type_column.unwrap_or_else(|| {
                DatasetVocabulary::DEFAULT_OPERATION_TYPE_COLUMN_NAME.to_string()
            }),
            system_time_column: v
                .system_time_column
                .unwrap_or_else(|| DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME.to_string()),
            event_time_column: v
                .event_time_column
                .unwrap_or_else(|| DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME.to_string()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for DatasetVocabulary {
    fn default() -> Self {
        Self {
            offset_column: Self::DEFAULT_OFFSET_COLUMN_NAME.to_string(),
            operation_type_column: Self::DEFAULT_OPERATION_TYPE_COLUMN_NAME.to_string(),
            system_time_column: Self::DEFAULT_SYSTEM_TIME_COLUMN_NAME.to_string(),
            event_time_column: Self::DEFAULT_EVENT_TIME_COLUMN_NAME.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReadStep {
    pub fn schema(&self) -> Option<&Vec<String>> {
        match self {
            ReadStep::Csv(v) => v.schema.as_ref(),
            ReadStep::Json(v) => v.schema.as_ref(),
            ReadStep::NdJson(v) => v.schema.as_ref(),
            ReadStep::GeoJson(v) => v.schema.as_ref(),
            ReadStep::NdGeoJson(v) => v.schema.as_ref(),
            ReadStep::EsriShapefile(v) => v.schema.as_ref(),
            ReadStep::Parquet(v) => v.schema.as_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepCsv
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepCsv {
    fn default() -> Self {
        Self {
            schema: None,
            separator: None,
            encoding: None,
            quote: None,
            escape: None,
            header: None,
            infer_schema: None,
            null_value: None,
            date_format: None,
            timestamp_format: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepJson
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepJson {
    fn default() -> Self {
        Self {
            sub_path: None,
            schema: None,
            date_format: None,
            encoding: None,
            timestamp_format: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdJson
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepNdJson {
    fn default() -> Self {
        Self {
            schema: None,
            date_format: None,
            encoding: None,
            timestamp_format: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepEsriShapefile
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepEsriShapefile {
    fn default() -> Self {
        Self {
            schema: None,
            sub_path: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Display for RawQueryResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for RawQueryResponseInternalError {
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

impl Display for TransformResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for TransformResponseInternalError {
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

impl DataSlice {
    pub fn num_records(&self) -> u64 {
        self.offset_interval.end - self.offset_interval.start + 1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SetDataSchema {
    #[cfg(feature = "arrow")]
    pub fn new(schema: &arrow::datatypes::Schema) -> Self {
        let mut encoder = arrow::ipc::convert::IpcSchemaEncoder::new();
        let (mut buf, head) = encoder.schema_to_fb(schema).collapse();
        buf.drain(0..head);
        Self { schema: buf }
    }

    #[cfg(feature = "arrow")]
    pub fn schema_as_arrow(&self) -> Result<arrow::datatypes::SchemaRef, crate::serde::Error> {
        let schema_proxy = flatbuffers::root::<arrow::ipc::gen::Schema::Schema>(&self.schema)
            .map_err(crate::serde::Error::serde)?;
        let schema = arrow::ipc::convert::fb_to_schema(schema_proxy);
        Ok(arrow::datatypes::SchemaRef::new(schema))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SourceState {
    pub const DEFAULT_SOURCE_NAME: &'static str = "default";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEventTypeFlags
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MetadataEventTypeFlags {
    pub const DATA_BLOCK: Self =
        Self::from_bits_retain(Self::ADD_DATA.bits() | Self::EXECUTE_TRANSFORM.bits());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

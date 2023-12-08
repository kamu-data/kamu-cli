// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::fmt::Display;

use crate::*;

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
////////////////////////////////////////////////////////////////////////////////

impl DatasetVocabulary {
    pub fn into_resolved(self) -> DatasetVocabularyResolvedOwned {
        self.into()
    }
}

impl Default for SetVocab {
    fn default() -> Self {
        Self {
            offset_column: None,
            system_time_column: None,
            event_time_column: None,
        }
    }
}

impl Default for DatasetVocabulary {
    fn default() -> Self {
        Self {
            offset_column: None,
            system_time_column: None,
            event_time_column: None,
        }
    }
}

impl From<SetVocab> for DatasetVocabulary {
    fn from(v: SetVocab) -> Self {
        Self {
            offset_column: v.offset_column,
            system_time_column: v.system_time_column,
            event_time_column: v.event_time_column,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetVocabularyResolved<'a> {
    /// Name of the system time column.
    pub system_time_column: Cow<'a, str>,
    /// Name of the event time column.
    pub event_time_column: Cow<'a, str>,
    /// Name of the offset column.
    pub offset_column: Cow<'a, str>,
}

pub type DatasetVocabularyResolvedOwned = DatasetVocabularyResolved<'static>;

impl Default for DatasetVocabularyResolvedOwned {
    fn default() -> Self {
        Self::from(DatasetVocabulary::default())
    }
}

impl<'a> From<&'a DatasetVocabulary> for DatasetVocabularyResolved<'a> {
    fn from(value: &'a DatasetVocabulary) -> Self {
        Self {
            system_time_column: Cow::Borrowed(
                value
                    .system_time_column
                    .as_ref()
                    .map(String::as_str)
                    .unwrap_or(DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME),
            ),
            event_time_column: Cow::Borrowed(
                value
                    .event_time_column
                    .as_ref()
                    .map(String::as_str)
                    .unwrap_or(DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME),
            ),
            offset_column: Cow::Borrowed(
                value
                    .offset_column
                    .as_ref()
                    .map(String::as_str)
                    .unwrap_or(DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME),
            ),
        }
    }
}

impl From<DatasetVocabulary> for DatasetVocabularyResolvedOwned {
    fn from(value: DatasetVocabulary) -> Self {
        Self {
            system_time_column: value
                .system_time_column
                .map(Cow::Owned)
                .unwrap_or(Cow::Borrowed(
                    DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME,
                )),

            event_time_column: value
                .event_time_column
                .map(Cow::Owned)
                .unwrap_or(Cow::Borrowed(
                    DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME,
                )),
            offset_column: value
                .offset_column
                .map(Cow::Owned)
                .unwrap_or(Cow::Borrowed(DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME)),
        }
    }
}

impl From<SetVocab> for DatasetVocabularyResolvedOwned {
    fn from(value: SetVocab) -> Self {
        let vocab: DatasetVocabulary = value.into();
        Self::from(vocab)
    }
}

impl<'a> DatasetVocabularyResolved<'a> {
    pub fn into_owned(self) -> DatasetVocabularyResolvedOwned {
        DatasetVocabularyResolvedOwned {
            system_time_column: self.system_time_column.into_owned().into(),
            event_time_column: self.event_time_column.into_owned().into(),
            offset_column: self.offset_column.into_owned().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TransformSql
////////////////////////////////////////////////////////////////////////////////

impl TransformSql {
    pub fn normalize_queries(mut self, implicit_alias: Option<String>) -> Self {
        if let Some(query) = self.query {
            assert!(!self.queries.is_some());

            self.queries = Some(vec![SqlQueryStep {
                alias: None,
                query: query.clone(),
            }]);

            self.query = None;
        }

        let nameless_queries = self
            .queries
            .as_ref()
            .unwrap()
            .iter()
            .map(|q| &q.alias)
            .filter(|a| a.is_none())
            .count();

        assert!(
            nameless_queries <= 1,
            "TransformSql has multiple queries without an alias"
        );

        if nameless_queries > 0 {
            for step in self.queries.as_mut().unwrap() {
                if step.alias.is_none() {
                    step.alias = implicit_alias;
                    break;
                }
            }
        }

        self
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
////////////////////////////////////////////////////////////////////////////////

impl ReadStep {
    pub fn schema(&self) -> Option<&Vec<String>> {
        match self {
            ReadStep::Csv(v) => v.schema.as_ref(),
            ReadStep::Json(v) => v.schema.as_ref(),
            ReadStep::JsonLines(v) => v.schema.as_ref(),
            ReadStep::NdJson(v) => v.schema.as_ref(),
            ReadStep::GeoJson(v) => v.schema.as_ref(),
            ReadStep::NdGeoJson(v) => v.schema.as_ref(),
            ReadStep::EsriShapefile(v) => v.schema.as_ref(),
            ReadStep::Parquet(v) => v.schema.as_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStepCsv
////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepCsv {
    fn default() -> Self {
        Self {
            schema: None,
            separator: None,
            encoding: None,
            quote: None,
            escape: None,
            comment: None,
            header: None,
            enforce_schema: None,
            infer_schema: None,
            ignore_leading_white_space: None,
            ignore_trailing_white_space: None,
            null_value: None,
            empty_value: None,
            nan_value: None,
            positive_inf: None,
            negative_inf: None,
            date_format: None,
            timestamp_format: None,
            multi_line: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStepJsonLines
////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepJsonLines {
    fn default() -> Self {
        Self {
            schema: None,
            date_format: None,
            encoding: None,
            multi_line: None,
            primitives_as_string: None,
            timestamp_format: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStepJson
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// ReadStepNdJson
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// ReadStepEsriShapefile
////////////////////////////////////////////////////////////////////////////////

impl Default for ReadStepEsriShapefile {
    fn default() -> Self {
        Self {
            schema: None,
            sub_path: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
////////////////////////////////////////////////////////////////////////////////

impl Display for ExecuteQueryResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for ExecuteQueryResponseInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)?;
        if let Some(bt) = &self.backtrace {
            write!(f, "\n\n--- Engine Backtrace ---\n{}", bt)?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
////////////////////////////////////////////////////////////////////////////////

impl SetDataSchema {
    #[cfg(feature = "arrow")]
    pub fn new(schema: &arrow::datatypes::Schema) -> Self {
        let (mut buf, head) = arrow::ipc::convert::schema_to_fb(schema).collapse();
        buf.drain(0..head);
        Self { schema: buf }
    }

    #[cfg(feature = "arrow")]
    pub fn schema_as_arrow(&self) -> Result<arrow::datatypes::SchemaRef, crate::serde::Error> {
        let schema_proxy = flatbuffers::root::<arrow::ipc::gen::Schema::Schema>(&self.schema)
            .map_err(|e| crate::serde::Error::serde(e))?;
        let schema = arrow::ipc::convert::fb_to_schema(schema_proxy);
        Ok(arrow::datatypes::SchemaRef::new(schema))
    }
}

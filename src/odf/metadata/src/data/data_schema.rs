// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::IntoExtraAttribute;
use crate::data::*;
use crate::dtos::datasets::DatasetVocabulary;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
    pub fn builder() -> DataSchemaBuilder {
        DataSchemaBuilder::new()
    }

    pub fn new_empty() -> Self {
        Self {
            fields: Vec::new(),
            extra: None,
        }
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        Self {
            fields,
            extra: None,
        }
    }

    pub fn new_with_attrs(fields: Vec<DataField>, extra: ExtraAttributes) -> Self {
        Self {
            fields,
            extra: extra.map_empty(),
        }
    }

    pub fn field_by_name(&self, name: &str) -> Option<&DataField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Removes fields that match their default values
    pub fn normalize(self) -> Self {
        Self {
            fields: self.fields.into_iter().map(DataField::normalize).collect(),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct DataSchemaBuilder {
    fields: Vec<DataField>,
    extra: ExtraAttributes,
}

impl DataSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "builder-style public API takes the vocabulary by value"
    )]
    pub fn with_changelog_system_fields(
        mut self,
        vocab: DatasetVocabulary,
        custom_event_time_type: Option<DataType>,
    ) -> Self {
        assert_eq!(self.fields.len(), 0);

        self.fields.extend([
            DataField::i64(vocab.offset_column()),
            DataField::i32(vocab.operation_type_column()),
            DataField::timestamp_millis_utc(vocab.system_time_column()),
            DataField::new(
                vocab.event_time_column(),
                custom_event_time_type.unwrap_or(DataType::timestamp_millis_utc()),
            ),
        ]);

        self
    }

    pub fn push(mut self, field: DataField) -> Self {
        self.fields.push(field);
        self
    }

    pub fn extend<I>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = DataField>,
    {
        self.fields.extend(fields);
        self
    }

    /// Serializes custom attribute and merges with existing
    pub fn extra<T: IntoExtraAttribute>(mut self, attr: T) -> Self {
        self.extra.insert(attr);
        self
    }

    /// Merges JSON object with existing attributes
    pub fn extra_json(mut self, attrs: serde_json::Value) -> Self {
        self.extra.insert_json(attrs);
        self
    }

    pub fn build(self) -> Result<DataSchema, InvalidSchema> {
        // TODO: Add validation
        // - duplicate fields
        // - system columns
        // - field types sanity
        Ok(DataSchema::new_with_attrs(self.fields, self.extra))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use kamu::domain::*;

use super::{CLIError, Command};
use crate::output::*;

pub struct SearchCommand {
    search_svc: Arc<dyn SearchService>,
    output_config: Arc<OutputConfig>,
    query: Option<String>,
    repository_names: Vec<odf::RepoName>,
}

impl SearchCommand {
    pub fn new<S, I>(
        search_svc: Arc<dyn SearchService>,
        output_config: Arc<OutputConfig>,
        query: Option<S>,
        repository_names: I,
    ) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = odf::RepoName>,
    {
        Self {
            search_svc,
            output_config,
            query: query.map(Into::into),
            repository_names: repository_names.into_iter().collect(),
        }
    }

    fn humanize_data_size(size: u64) -> String {
        if size == 0 {
            return "-".to_owned();
        }
        use humansize::{format_size, BINARY};
        format_size(size, BINARY)
    }

    fn humanize_quantity(num: u64) -> String {
        use num_format::{Locale, ToFormattedString};
        if num == 0 {
            return "-".to_owned();
        }
        num.to_formatted_string(&Locale::en)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SearchCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let mut result = self
            .search_svc
            .search(
                self.query.as_deref(),
                SearchOptions {
                    repository_names: self.repository_names.clone(),
                },
            )
            .await
            .map_err(CLIError::failure)?;

        result.datasets.sort_by(|a, b| a.alias.cmp(&b.alias));

        let schema = Arc::new(Schema::new(vec![
            Field::new("Alias", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, true),
            Field::new("Description", DataType::Utf8, true),
            Field::new("Blocks", DataType::UInt64, true),
            Field::new("Records", DataType::UInt64, true),
            Field::new("Size", DataType::UInt64, true),
        ]));

        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default().with_null_value("-"))
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"), // Alias
                ColumnFormat::new().with_style_spec("c"), // Kind
                ColumnFormat::new().with_style_spec("l"), // Description
                ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(Self::humanize_quantity), // Blocks
                ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(Self::humanize_quantity), // Records
                ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(Self::humanize_data_size), // Size
            ]);

        let mut alias = Vec::new();
        let mut kind = Vec::new();
        let mut description = Vec::new();
        let mut blocks = Vec::new();
        let mut records = Vec::new();
        let mut size = Vec::new();

        for ds in result.datasets {
            alias.push(ds.alias.to_string());
            kind.push(ds.kind.map(|k| format!("{k:?}")));
            description.push(Option::<String>::None);
            blocks.push(ds.num_blocks);
            records.push(ds.num_records);
            size.push(ds.estimated_size);
        }

        let records = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(alias)),
                Arc::new(StringArray::from(kind)),
                Arc::new(StringArray::from(description)),
                Arc::new(UInt64Array::from(blocks)),
                Arc::new(UInt64Array::from(records)),
                Arc::new(UInt64Array::from(size)),
            ],
        )
        .unwrap();

        let mut writer = self
            .output_config
            .get_records_writer(&schema, records_format);
        writer.write_batch(&records)?;
        writer.finish()?;

        Ok(())
    }
}

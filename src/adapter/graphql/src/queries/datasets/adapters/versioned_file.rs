// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use super::{FileVersion, VersionedFileEntry, VersionedFileEntryConnection};
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFile<'a> {
    state: &'a DatasetRequestState,
}

impl<'a> VersionedFile<'a> {
    pub fn new(state: &'a DatasetRequestState) -> Self {
        Self { state }
    }

    pub fn dataset_snapshot(
        alias: odf::DatasetAlias,
        extra_columns: Vec<ColumnInput>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::InvalidSchema> {
        let extra_columns_ddl: Vec<String> = extra_columns
            .into_iter()
            .map(|c| format!("{} {}", c.name, c.data_type.ddl))
            .collect();

        let extra_columns_schema =
            odf::utils::schema::parse::parse_ddl_to_odf_schema(&extra_columns_ddl.join(", "))?;

        let schema = odf::schema::DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                odf::schema::DataField::i32("version").description(
                    "Sequential identifier assigned to each entry as new versions are uploaded",
                ),
                odf::schema::DataField::string("content_hash")
                    .type_ext(odf::schema::ext::DataTypeExt::object_link(
                        odf::schema::ext::DataTypeExt::multihash(),
                    ))
                    .description("Hash that references the externally-stored object content"),
                odf::schema::DataField::i64("content_length")
                    .description("Size of the linked object in bytes"),
                odf::schema::DataField::string("content_type")
                    .optional()
                    .description("Media type associated with the linked object"),
            ])
            .extend(extra_columns_schema.fields)
            .extra(odf::schema::ext::DatasetArchetype::VersionedFile)
            .build()?;

        let push_source = odf::metadata::AddPushSource {
            source_name: "default".into(),
            read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                schema: Some(
                    [
                        "version INT",
                        "content_hash STRING",
                        "content_length BIGINT",
                        "content_type STRING",
                    ]
                    .into_iter()
                    .map(str::to_string)
                    .chain(extra_columns_ddl)
                    .collect(),
                ),
                ..Default::default()
            }),
            preprocess: None,
            merge: odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
        };

        Ok(odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: [
                odf::MetadataEvent::SetDataSchema(odf::metadata::SetDataSchema::new(schema)),
                odf::MetadataEvent::AddPushSource(push_source),
            ]
            .into_iter()
            .chain(extra_events)
            .collect(),
        })
    }

    pub async fn get_entry(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<Option<VersionedFileEntry>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let query_res = if let Some(block_hash) = as_of_block_hash {
            query_svc
                .tail_old(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions {
                        block_hash: Some(block_hash.into()),
                    },
                )
                .await
        } else if let Some(version) = as_of_version {
            use datafusion::logical_expr::{col, lit};

            query_svc
                .get_data_old(
                    &self.state.dataset_handle().as_local_ref(),
                    domain::GetDataOptions::default(),
                )
                .await
                .map(|res| domain::GetDataResponse {
                    df: res
                        .df
                        .map(|df| df.filter(col("version").eq(lit(version))).unwrap()),
                    dataset_handle: res.dataset_handle,
                    block_hash: res.block_hash,
                })
        } else {
            query_svc
                .tail_old(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions::default(),
                )
                .await
        }
        .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);

        let dataset = self.state.resolved_dataset(ctx).await?;
        let entry =
            VersionedFileEntry::from_json(dataset.clone(), records.into_iter().next().unwrap())?;

        Ok(Some(entry))
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl VersionedFile<'_> {
    const DEFAULT_VERSIONS_PER_PAGE: usize = 100;

    /// Returns list of versions in reverse chronological order
    #[tracing::instrument(level = "info", name = VersionedFile_versions, skip_all)]
    pub async fn versions(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Upper version bound (inclusive)")] max_version: Option<FileVersion>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<VersionedFileEntryConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_VERSIONS_PER_PAGE);

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let query_res = query_svc
            .get_data_old(
                &self.state.dataset_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(VersionedFileEntryConnection::new(
                Vec::new(),
                page,
                per_page,
                0,
            ));
        };

        let df = if let Some(max_version) = max_version {
            df.filter(col("version").lt_eq(lit(max_version)))
                .int_err()?
        } else {
            df
        };

        let total_count = df.clone().count().await.int_err()?;

        let df = df
            .sort(vec![col("version").sort(false, false)])
            .int_err()?
            .limit(page * per_page, Some(per_page))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let dataset = self.state.resolved_dataset(ctx).await?;
        let nodes = records
            .into_iter()
            .map(|r| VersionedFileEntry::from_json(dataset.clone(), r))
            .collect::<Result<_, _>>()?;

        Ok(VersionedFileEntryConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Returns the latest version entry, if any
    #[tracing::instrument(level = "info", name = VersionedFile_latest, skip_all)]
    pub async fn latest(&self, ctx: &Context<'_>) -> Result<Option<VersionedFileEntry>> {
        self.get_entry(ctx, None, None).await
    }

    /// Returns the specified entry by block or version number
    #[tracing::instrument(level = "info", name = VersionedFile_as_of, skip_all)]
    pub async fn as_of(
        &self,
        ctx: &Context<'_>,
        version: Option<FileVersion>,
        block_hash: Option<Multihash<'static>>,
    ) -> Result<Option<VersionedFileEntry>> {
        self.get_entry(ctx, version, block_hash).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

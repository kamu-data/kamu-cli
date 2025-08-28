// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use super::{CollectionEntry, CollectionEntryConnection};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Collection {
    dataset: domain::ResolvedDataset,
}

impl Collection {
    pub fn dataset_snapshot(
        alias: odf::DatasetAlias,
        extra_columns: Vec<ColumnInput>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::UnsupportedSchema> {
        let extra_columns_ddl: Vec<String> = extra_columns
            .into_iter()
            .map(|c| format!("{} {}", c.name, c.data_type.ddl))
            .collect();

        let extra_columns_schema =
            odf::utils::schema::parse::parse_ddl_to_odf_schema(&extra_columns_ddl.join(", "))?;

        let schema = odf::schema::DataSchema::new(
            [
                odf::schema::DataField::i64("offset"),
                odf::schema::DataField::i32("op"),
                odf::schema::DataField::timestamp_millis_utc("system_time"),
                odf::schema::DataField::timestamp_millis_utc("event_time"),
                odf::schema::DataField::string("path").description(
                    "HTTP-like path to a collection entry. Paths start with `/` and can be \
                     nested, with individual path segments url-encoded (e.g. `/foo/bar%20baz`)",
                ),
                odf::schema::DataField::string("ref")
                    .type_ext(odf::schema::ext::DataTypeExt::did())
                    .description("DID that references another dataset"),
            ]
            .into_iter()
            .chain(extra_columns_schema.fields)
            .collect(),
        )
        .extra(&odf::schema::ext::AttrArchetype::new(
            odf::schema::ext::DatasetArchetype::Collection,
        ));

        let push_source = odf::metadata::AddPushSource {
            source_name: "default".into(),
            read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                schema: Some(
                    ["op INT", "path STRING", "ref STRING"]
                        .into_iter()
                        .map(str::to_string)
                        .chain(extra_columns_ddl)
                        .collect(),
                ),
                ..Default::default()
            }),
            preprocess: None,
            merge: odf::metadata::MergeStrategy::ChangelogStream(
                odf::metadata::MergeStrategyChangelogStream {
                    primary_key: vec!["path".to_string()],
                },
            ),
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
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Collection {
    #[graphql(skip)]
    pub fn new(dataset: domain::ResolvedDataset) -> Self {
        Self { dataset }
    }

    /// Latest state projection of the state of a collection
    #[tracing::instrument(level = "info", name = Collection_latest, skip_all)]
    pub async fn latest(&self) -> CollectionProjection {
        CollectionProjection::new(self.dataset.clone(), None)
    }

    /// State projection of the state of a collection at the specified point in
    /// time
    #[tracing::instrument(level = "info", name = Collection_as_of, skip_all)]
    pub async fn as_of(&self, block_hash: Multihash<'static>) -> CollectionProjection {
        CollectionProjection::new(self.dataset.clone(), Some(block_hash.into()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionProjection {
    dataset: domain::ResolvedDataset,
    as_of: Option<odf::Multihash>,
}

impl CollectionProjection {
    pub fn new(dataset: domain::ResolvedDataset, as_of: Option<odf::Multihash>) -> Self {
        Self { dataset, as_of }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl CollectionProjection {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    /// Returns an entry at the specified path
    #[tracing::instrument(level = "info", name = CollectionProjection_entry, skip_all)]
    pub async fn entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let Some(df) = query_svc
            .get_data(
                &self.dataset.get_handle().as_local_ref(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df
        else {
            return Ok(None);
        };

        // Apply filters
        // Note: we are still working with a changelog here in hope to narrow down the
        // record set before projecting
        let df = df.filter(col("path").eq(lit(path.to_string()))).int_err()?;

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let record = records.into_iter().next().unwrap();
        let entry = CollectionEntry::from_json(record)?;

        Ok(Some(entry))
    }

    /// Returns the state of entries as they existed at a specified point in
    /// time
    #[tracing::instrument(level = "info", name = CollectionProjection_entries, skip_all)]
    pub async fn entries(
        &self,
        ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<CollectionEntryConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let df = query_svc
            .get_data(
                &self.dataset.get_handle().as_local_ref(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df;

        let Some(df) = df else {
            return Ok(CollectionEntryConnection::new(Vec::new(), 0, 0, 0));
        };

        // Apply filters
        // Note: we are still working with a changelog here in the hope to narrow down
        // the record set before projecting
        let df = match path_prefix {
            None => df,
            Some(path_prefix) => df
                .filter(
                    datafusion::functions::string::starts_with()
                        .call(vec![col("path"), lit(path_prefix.to_string())]),
                )
                .int_err()?,
        };

        let df = match max_depth {
            None => df,
            Some(_) => unimplemented!(),
        };

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let total_count = df.clone().count().await.int_err()?;
        let df = df
            .sort(vec![col("path").sort(true, false)])
            .int_err()?
            .limit(page * per_page, Some(per_page))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let nodes = records
            .into_iter()
            .map(CollectionEntry::from_json)
            .collect::<Result<_, _>>()?;

        Ok(CollectionEntryConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Find entries that link to specified DIDs
    #[tracing::instrument(level = "info", name = CollectionProjection_entries_by_ref, skip_all, fields(refs))]
    pub async fn entries_by_ref(
        &self,
        ctx: &Context<'_>,
        refs: Vec<DatasetID<'_>>,
    ) -> Result<Vec<CollectionEntry>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let Some(df) = query_svc
            .get_data(
                &self.dataset.get_handle().as_local_ref(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df
        else {
            return Ok(Vec::new());
        };

        // Apply filters
        // Note: we are still working with a changelog here in hope to narrow down the
        // record set before projecting
        let df = df
            .filter(col("ref").in_list(
                refs.into_iter().map(|r| lit(r.to_string())).collect(),
                false,
            ))
            .int_err()?;

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let df = df.sort(vec![col("path").sort(true, false)]).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let nodes = records
            .into_iter()
            .map(CollectionEntry::from_json)
            .collect::<Result<_, _>>()?;

        Ok(nodes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

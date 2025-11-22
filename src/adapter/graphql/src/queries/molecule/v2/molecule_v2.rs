// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::DatasetAction;

use crate::prelude::*;
use crate::queries::molecule::v1::MoleculeV1;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntryV2,
    MoleculeCategoryV2,
    MoleculeProjectActivityFiltersV2,
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeTagV2,
    MoleculeVersionedFileV2,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeV2 {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;
    // const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectV2>> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = MoleculeV1::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(None);
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let entry = MoleculeProjectV2::from_json(records.into_iter().next().unwrap())?;

        Ok(Some(entry))
    }

    /// List the registered projects
    #[tracing::instrument(level = "info", name = MoleculeV2_projects, skip_all)]
    async fn projects(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectV2Connection> {
        use datafusion::logical_expr::col;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PROJECTS_PER_PAGE);

        let Some(df) = MoleculeV1::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(MoleculeProjectV2Connection::new(Vec::new(), 0, per_page, 0));
        };

        let total_count = df.clone().count().await.int_err()?;
        let df = df
            .sort(vec![col("ipnft_symbol").sort(true, false)])
            .int_err()?
            .limit(page * per_page, Some(per_page))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let nodes = records
            .into_iter()
            .map(MoleculeProjectV2::from_json)
            .collect::<Result<Vec<MoleculeProjectV2>, _>>()?;

        Ok(MoleculeProjectV2Connection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = MoleculeV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeProjectActivityFiltersV2>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        let _ = page;
        let _ = per_page;
        let _ = filters;
        // TODO: implement
        Ok(MoleculeActivityEventV2Connection::new(Vec::new(), 0, 0))
    }

    /// Performs a semantic search
    /// Using `filters` parameters, a search can be narrowed down to:
    /// - a specific set of projects
    /// - specific categories and tags
    /// - only returning files or announcements
    #[tracing::instrument(level = "info", name = MoleculeV2_search, skip_all)]
    async fn search(
        &self,
        _ctx: &Context<'_>,
        // TODO: update types
        prompt: String,
        filters: Option<MoleculeSemanticSearchFiltersV2>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeSemanticSearchFoundItemV2Connection> {
        let _ = prompt;
        let _ = filters;
        let _ = page;
        let _ = per_page;
        // TODO: implement
        Ok(MoleculeSemanticSearchFoundItemV2Connection::new(
            vec![],
            0,
            0,
            0,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeSemanticSearchFiltersV2 {
    // TODO: replace w/ real filters.
    // These filters are provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
    by_categories: Option<Vec<MoleculeCategoryV2>>,
    by_tags: Option<Vec<MoleculeTagV2>>,
}

#[derive(Union)]
pub enum MoleculeSemanticSearchFoundItemV2 {
    File(MoleculeSemanticSearchFoundFileV2),
    Announcement(MoleculeSemanticSearchFoundAnnouncementV2),
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundFileV2 {
    pub entry: MoleculeVersionedFileV2,
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundAnnouncementV2 {
    pub entry: MoleculeAnnouncementEntryV2,
}

page_based_connection!(
    MoleculeSemanticSearchFoundItemV2,
    MoleculeSemanticSearchFoundItemV2Connection,
    MoleculeSemanticSearchFoundItemV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

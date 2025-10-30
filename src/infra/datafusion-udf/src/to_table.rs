// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult, exec_err, plan_err};
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::*;
use kamu_core::{GetDataWithMetadataOptions, QueryError, QueryService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FUNCTION_NAME: &str = "to_table";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// IMPORTANT: Multi-threaded tokio runtime is required for operation!
pub struct ToTableUdtf {
    query_svc: Arc<dyn QueryService>,
}

impl ToTableUdtf {
    pub fn register(ctx: &SessionContext, query_svc: Arc<dyn QueryService>) {
        ctx.register_udtf(FUNCTION_NAME, Arc::new(Self::new(query_svc)));
    }

    pub fn new(query_svc: Arc<dyn QueryService>) -> Self {
        Self { query_svc }
    }

    async fn main(&self, dataset_ref: odf::DatasetRef) -> DfResult<Arc<dyn TableProvider>> {
        let data_response = self
            .query_svc
            .get_data_with_metadata(
                &dataset_ref,
                GetDataWithMetadataOptions::builder()
                    .resolve_dataset_vocabulary(true)
                    .resolve_merge_strategy(true)
                    .build(),
            )
            .await
            .map_err(|e| {
                // Rewrite access error
                if let QueryError::Access(_) = e {
                    odf::DatasetNotFoundError::new(dataset_ref.clone()).into()
                } else {
                    e
                }
            })
            .map_err(|e| DataFusionError::External(e.into()))?;

        let Some(df) = data_response.df else {
            // Dataset has no schema yet -- return empty table
            return Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))));
        };

        let Some(primary_key) = data_response
            .merge_strategy
            .and_then(odf::metadata::MergeStrategy::primary_key)
        else {
            return exec_err!("{FUNCTION_NAME}: {dataset_ref} has no primary key");
        };
        let dataset_vocabulary = data_response.dataset_vocabulary.unwrap_or_default();

        let projection_df =
            odf::utils::data::changelog::project(df, &primary_key, &dataset_vocabulary)?;

        Ok(projection_df.into_view())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: Required for TableFunctionImpl (DataFusion trait)
impl std::fmt::Debug for ToTableUdtf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToTableUdtf").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ToTableFunctionArgs {
    dataset_ref: odf::DatasetRef,
}

impl TryFrom<&[Expr]> for ToTableFunctionArgs {
    type Error = DataFusionError;

    fn try_from(args: &[Expr]) -> Result<Self, Self::Error> {
        if args.len() != 1 {
            return plan_err!("{FUNCTION_NAME}() accepts only one argument: dataset_ref");
        }

        let dataset_ref_as_str = match args.first() {
            Some(Expr::Column(c)) if c.relation.is_none() && !c.name.is_empty() => &c.name,
            _ => {
                return plan_err!("{FUNCTION_NAME}(): dataset_ref must be a table reference");
            }
        };

        let Ok(dataset_ref) = dataset_ref_as_str.parse::<odf::DatasetRef>() else {
            return plan_err!(
                "{FUNCTION_NAME}() requires correct dataset_ref format but got: \
                 {dataset_ref_as_str}"
            );
        };

        Ok(Self { dataset_ref })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TableFunctionImpl for ToTableUdtf {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        let ToTableFunctionArgs { dataset_ref } = args.try_into()?;

        // Tricky code: we need schema beforehand which needs dataset itself,
        //              and as the call needs to be async, we are launching it here
        //              in a workaround way.

        // TODO: Remove this workaround once the following issue is resolved:
        //       Construction of user-defined table functions (UDTFs) should be async
        //       to allow for async schemas
        //       https://github.com/apache/datafusion/issues/10889

        // IMPORTANT: Multi-threaded tokio runtime is required for operation!
        tokio::task::block_in_place(move || {
            let runtime = tokio::runtime::Handle::current();
            runtime.block_on(async move { self.main(dataset_ref).await })
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

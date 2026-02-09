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
use datafusion::common::{DataFusionError as DFError, Result as DfResult, plan_err};
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::*;
use kamu_core::QueryError;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FUNCTION_NAME: &str = "to_table";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// IMPORTANT: Multi-threaded tokio runtime is required for operation!
pub struct ToTableUdtf<F>
where
    F: AsyncFn(odf::DatasetRef) -> Result<Option<DataFrameExt>, QueryError>,
{
    on_resolve_dataset_callback: F,
}

impl<F> ToTableUdtf<F>
where
    F: AsyncFn(odf::DatasetRef) -> Result<Option<DataFrameExt>, QueryError> + Sync + Send + 'static,
{
    pub fn register(ctx: &SessionContext, on_resolve_dataset_callback: F) {
        ctx.register_udtf(
            FUNCTION_NAME,
            Arc::new(Self::new(on_resolve_dataset_callback)),
        );
    }

    pub fn new(on_resolve_dataset_callback: F) -> Self {
        Self {
            on_resolve_dataset_callback,
        }
    }

    async fn main(&self, dataset_ref: odf::DatasetRef) -> DfResult<Arc<dyn TableProvider>> {
        let maybe_df = (self.on_resolve_dataset_callback)(dataset_ref.clone())
            .await
            .map_err(|e| {
                // Rewrite access error
                if let QueryError::Access(_) = e {
                    odf::DatasetNotFoundError::new(dataset_ref).into()
                } else {
                    e
                }
            })
            .map_err(|e| DFError::External(Box::new(e)))?;

        if let Some(df) = maybe_df {
            Ok(df.into_view())
        } else {
            // Dataset has no schema yet -- return empty table
            Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: Required for TableFunctionImpl (DataFusion trait)
impl<F> std::fmt::Debug for ToTableUdtf<F>
where
    F: AsyncFn(odf::DatasetRef) -> Result<Option<DataFrameExt>, QueryError>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToTableUdtf").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ToTableFunctionArgs {
    dataset_ref: odf::DatasetRef,
}

impl TryFrom<&[Expr]> for ToTableFunctionArgs {
    type Error = DFError;

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
                 \"{dataset_ref_as_str}\""
            );
        };

        Ok(Self { dataset_ref })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<F> TableFunctionImpl for ToTableUdtf<F>
where
    F: AsyncFn(odf::DatasetRef) -> Result<Option<DataFrameExt>, QueryError> + Sync + Send + 'static,
{
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

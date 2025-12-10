// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    ExtraDataFieldFilter,
    ExtraDataFieldsFilter,
    UnknownExtraDataFieldFilterNamesError,
};
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DataFrameExtraDataFieldsFilterApplier;

impl DataFrameExtraDataFieldsFilterApplier {
    fn validate_requested_extra_data_fields(
        df: &DataFrameExt,
        filter: &ExtraDataFieldsFilter,
    ) -> Result<(), DataFrameExtraDataFieldsFilterApplyError> {
        let available_fields = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<HashSet<_>>();
        let requested_fields = filter.iter().map(|f| &f.field_name).collect::<HashSet<_>>();

        let missing_fields = requested_fields
            .difference(&available_fields)
            .map(|f| (*f).clone())
            .collect::<Vec<_>>();

        if !missing_fields.is_empty() {
            Err(UnknownExtraDataFieldFilterNamesError {
                field_names: missing_fields,
            }
            .into())
        } else {
            Ok(())
        }
    }

    pub fn apply(
        df: DataFrameExt,
        filter: ExtraDataFieldsFilter,
    ) -> Result<DataFrameExt, DataFrameExtraDataFieldsFilterApplyError> {
        use datafusion::logical_expr::{Expr, col, lit};

        Self::validate_requested_extra_data_fields(&df, &filter)?;

        let filter_expr = filter
            .into_iter()
            .map(|ExtraDataFieldFilter { field_name, values }| {
                let values_as_lits = values.into_iter().map(lit).collect();
                // field1 in [1, 2, 3]
                col(field_name).in_list(values_as_lits, false)
            })
            // ((field1 in [1, 2, 3] AND field2 in [4, 5, 6]) AND field3 in [7, 8, 9])
            .reduce(Expr::and)
            // Safety: we use the NonEmpty<T>, so we will always have elements.
            .unwrap();

        let df = df.filter(filter_expr).int_err()?;

        Ok(df)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum DataFrameExtraDataFieldsFilterApplyError {
    #[error(transparent)]
    UnknownExtraDataFieldFilterNames(#[from] UnknownExtraDataFieldFilterNamesError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

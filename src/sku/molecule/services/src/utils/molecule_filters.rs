// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::logical_expr::{Expr, col, lit};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_molecule_domain::{MoleculeAccessLevelRule, utils};
use nonempty::NonEmpty;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Applies common molecule filters (tags/categories/ipnft/access-level rules)
/// to the provided dataframe.
pub fn apply_molecule_filters_to_df(
    df: DataFrameExt,
    by_ipnft_uids: Option<Vec<String>>,
    by_tags: Option<Vec<String>>,
    by_categories: Option<Vec<String>>,
    by_access_levels: Option<Vec<String>>,
    by_access_level_rules: Option<Vec<MoleculeAccessLevelRule>>,
) -> Result<DataFrameExt, InternalError> {
    let df = if let Some(filter) =
        utils::molecule_fields_filter(by_ipnft_uids, by_tags, by_categories)
    {
        kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filter)
            .int_err()?
    } else {
        df
    };

    let access_rules = utils::normalize_access_level_rules(by_access_levels, by_access_level_rules);

    apply_access_level_rules(df, access_rules)
}

fn apply_access_level_rules(
    df: DataFrameExt,
    rules: Vec<MoleculeAccessLevelRule>,
) -> Result<DataFrameExt, InternalError> {
    let filter_expr = rules
        .into_iter()
        .filter_map(|rule| {
            NonEmpty::from_vec(rule.access_levels).map(|access_levels| {
                let access_expr = col("molecule_access_level")
                    .in_list(access_levels.into_iter().map(lit).collect(), false);

                if let Some(ipnft_uid) = rule.ipnft_uid {
                    access_expr.and(col("ipnft_uid").eq(lit(ipnft_uid)))
                } else {
                    access_expr
                }
            })
        })
        .reduce(Expr::or);

    if let Some(expr) = filter_expr {
        Ok(df.filter(expr).int_err()?)
    } else {
        Ok(df)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

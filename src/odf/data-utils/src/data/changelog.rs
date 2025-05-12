// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use odf_metadata::{DatasetVocabulary, OperationType};

use super::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Projects the CDC ledger into a state snapshot.
///
/// Implementation is mostly equivalent to this example (using
/// datafusion-cli):
///
/// ```text
/// create or replace table ledger (
///     offset bigint not null,
///     op string not null,
///     city string not null, -- PK
///     population int not null
/// ) as values
///   (0, '+A', 'a', 1000),
///   (1, '+A', 'b', 2000),
///   (2, '+A', 'c', 3000),
///   (3, '-C', 'b', 2000),
///   (4, '+C', 'b', 2500),
///   (5, '-C', 'a', 1000),
///   (6, '+C', 'a', 1500),
///   (7, '-R', 'a', 1500);
///
/// select * from (
///    select
///        *,
///        row_number() over (
///            partition by city
///            order by offset desc
///        ) as __rank
///     from ledger
/// ) where __rank = 1 and op != '-R';
/// ```
///
/// Which should output:
///
/// ```text
/// +--------+----+------+------------+--------+
/// | offset | op | city | population | __rank |
/// +--------+----+------+------------+--------+
/// | 2      | +I | c    | 3000       | 1      |
/// | 3      | +C | b    | 2500       | 1      |
/// +--------+----+------+------------+--------+
/// ```
pub fn project(
    ledger: DataFrameExt,
    primary_key: &[String],
    vocab: &DatasetVocabulary,
) -> Result<DataFrameExt, DataFusionError> {
    // TODO: PERF: Re-assess implementation as it may be sub-optimal
    let rank_col = "__rank";

    let state = ledger
        .window(vec![datafusion::functions_window::row_number::row_number()
            .partition_by(
                primary_key
                    .iter()
                    .map(|name| col(Column::from_name(name)))
                    .collect(),
            )
            .order_by(vec![
                col(Column::from_name(&vocab.offset_column)).sort(false, false)
            ])
            .build()?
            .alias(rank_col)])?
        .filter(
            col(Column::from_name(rank_col)).eq(lit(1)).and(
                // TODO: Cast to `u8` after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                col(Column::from_name(&vocab.operation_type_column))
                    .not_eq(lit(OperationType::Retract as i32)),
            ),
        )?
        .without_columns(&[rank_col])?;

    Ok(state)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

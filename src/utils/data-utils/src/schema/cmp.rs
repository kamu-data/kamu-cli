// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};

/// Compare schemas optionally performing some normalization
pub fn assert_schemas_equal(lhs: &Schema, rhs: &Schema, ignore_nullability: bool) {
    let map_field = |f: &Arc<Field>| -> Arc<Field> {
        if ignore_nullability {
            Arc::new(f.as_ref().clone().with_nullable(true))
        } else {
            f.clone()
        }
    };

    let lhs = Schema::new_with_metadata(
        lhs.fields().iter().map(map_field).collect::<Vec<_>>(),
        lhs.metadata().clone(),
    );
    let rhs = Schema::new_with_metadata(
        rhs.fields().iter().map(map_field).collect::<Vec<_>>(),
        rhs.metadata().clone(),
    );
    assert_eq!(lhs, rhs);
}

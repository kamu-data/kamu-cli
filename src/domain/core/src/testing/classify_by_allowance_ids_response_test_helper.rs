// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::auth::ClassifyByAllowanceIdsResponse;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ClassifyByAllowanceIdsResponseTestHelper;

impl ClassifyByAllowanceIdsResponseTestHelper {
    pub fn report(
        mut response: ClassifyByAllowanceIdsResponse,
        dataset_handle_map: &HashMap<odf::DatasetID, odf::DatasetAlias>,
    ) -> String {
        response.authorized_ids.sort();
        response
            .unauthorized_ids_with_errors
            .sort_by(|(left_id, _), (right_id, _)| left_id.cmp(right_id));

        let mut res = "authorized:\n".to_string();
        for id in response.authorized_ids {
            res += &if let Some(alias) = dataset_handle_map.get(&id) {
                format!("- {alias}\n")
            } else {
                format!("- {id}\n")
            };
        }
        res += "\nunauthorized_with_errors:\n";
        for (id, e) in response.unauthorized_ids_with_errors {
            res += &if let Some(alias) = dataset_handle_map.get(&id) {
                format!("- {alias}: {e}\n")
            } else {
                format!("- {id}: {e}\n")
            };
        }

        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::ViewMultiResponse;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ViewMultiResponseTestHelper;

impl ViewMultiResponseTestHelper {
    pub fn report(mut response: ViewMultiResponse) -> String {
        response
            .viewable_resolved_refs
            .sort_by(|(left_ref, _), (right_ref, _)| left_ref.cmp(right_ref));
        response
            .inaccessible_refs
            .sort_by(|(left_ref, _), (right_ref, _)| left_ref.cmp(right_ref));

        let mut res = "viewable_resolved_refs:\n".to_string();
        for (resolved_ref, _) in response.viewable_resolved_refs {
            res += &format!("- {resolved_ref}\n");
        }
        res += "\ninaccessible_refs:\n";
        for (resolved_ref, e) in response.inaccessible_refs {
            res += &format!("- {resolved_ref}: {e}\n");
        }
        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

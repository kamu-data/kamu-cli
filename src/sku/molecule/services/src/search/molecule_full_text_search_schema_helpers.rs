// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{
    MoleculeProjectEntity,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_project(project: &MoleculeProjectEntity) -> serde_json::Value {
    serde_json::json!({
        project_schema::FIELD_IPNFT_SYMBOL: project.ipnft_symbol,
        project_schema::FIELD_IPNFT_UID: project.ipnft_uid,
        project_schema::FIELD_ACCOUNT_ID: project.account_id,
        project_schema::FIELD_CREATED_AT: project.system_time,
        project_schema::FIELD_UPDATED_AT: project.system_time,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

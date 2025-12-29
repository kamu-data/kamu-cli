// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::{
    MoleculeProject,
    MoleculeViewProjectsUseCase,
    molecule_project_search_schema as project_schema,
    molecule_search_schema_common as molecule_schema,
};
use kamu_search::{SearchIndexUpdateOperation, SearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 500;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_project_from_entity(
    molecule_account_id: &odf::AccountID,
    project: &MoleculeProject,
) -> serde_json::Value {
    serde_json::json!({
        molecule_schema::fields::EVENT_TIME: project.event_time,
        molecule_schema::fields::SYSTEM_TIME: project.system_time,
        molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id.to_string(),
        project_schema::fields::IPNFT_SYMBOL: project.ipnft_symbol,
        molecule_schema::fields::IPNFT_UID: project.ipnft_uid,
        project_schema::fields::PROJECT_ACCOUNT_ID: project.account_id,
        kamu_search::SEARCH_FIELD_IS_BANNED: false,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_project_from_parts(
    molecule_account_id: &odf::AccountID,
    ipnft_uid: &str,
    ipnft_symbol: &str,
    account_id: &odf::AccountID,
    event_time: DateTime<Utc>,
    system_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        molecule_schema::fields::EVENT_TIME: event_time,
        molecule_schema::fields::SYSTEM_TIME: system_time,
        molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id.to_string(),
        project_schema::fields::IPNFT_SYMBOL: ipnft_symbol,
        molecule_schema::fields::IPNFT_UID: ipnft_uid,
        project_schema::fields::PROJECT_ACCOUNT_ID: account_id,
        kamu_search::SEARCH_FIELD_IS_BANNED: false,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_project_when_ban_status_changed(
    is_banned: bool,
    event_time: DateTime<Utc>,
    system_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        molecule_schema::fields::EVENT_TIME: event_time,
        molecule_schema::fields::SYSTEM_TIME: system_time,
        kamu_search::SEARCH_FIELD_IS_BANNED: is_banned,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_projects(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn SearchRepository,
) -> Result<usize, InternalError> {
    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        "Indexing projects for Molecule organization account",
    );

    let mut total_documents_count = 0;
    let mut operations = Vec::new();

    let molecule_view_projects_uc = catalog
        .get_one::<dyn MoleculeViewProjectsUseCase>()
        .unwrap();

    // Load all projects for the organization account
    let projects_listing = molecule_view_projects_uc
        .execute(organization_account, None)
        .await
        .int_err()?;

    // Index each project
    for project in projects_listing.list {
        // Serialize project into search document
        let document = index_project_from_entity(&organization_account.account_id, &project);

        // Note: for now, use IPNFT UID as document ID
        // This should be revised after implementing non-tokenized projects
        operations.push(SearchIndexUpdateOperation::Index {
            id: project.ipnft_uid.clone(),
            doc: document,
        });

        // Bulk index when we reach BULK_SIZE
        if operations.len() >= BULK_SIZE {
            tracing::debug!(
                documents_count = operations.len(),
                "Bulk indexing projects batch",
            );
            repo.bulk_update(project_schema::SCHEMA_NAME, operations)
                .await?;
            total_documents_count += BULK_SIZE;
            operations = Vec::new();
        }
    }

    // Index remaining documents
    if !operations.is_empty() {
        let remaining_count = operations.len();
        tracing::debug!(
            documents_count = remaining_count,
            "Bulk indexing final projects batch",
        );
        repo.bulk_update(project_schema::SCHEMA_NAME, operations)
            .await?;
        total_documents_count += remaining_count;
    }

    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        indexed_documents_count = total_documents_count,
        "Indexed projects for Molecule organization account",
    );

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

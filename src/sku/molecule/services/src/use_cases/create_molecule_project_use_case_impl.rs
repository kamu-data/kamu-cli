// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::{
    AccountService,
    AccountServiceExt,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    LoggedAccount,
};
use kamu_auth_rebac::RebacService;
use kamu_core::PushIngestDataUseCase;
use kamu_core::auth::DatasetAction;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetUseCaseOptions};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn CreateMoleculeProjectUseCase)]
pub struct CreateMoleculeProjectUseCaseImpl {
    project_service: Arc<dyn MoleculeProjectService>,
    account_service: Arc<dyn AccountService>,
    create_account_use_case: Arc<dyn CreateAccountUseCase>,
    create_dataset_from_snapshot_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
    rebac_service: Arc<dyn RebacService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CreateMoleculeProjectUseCase for CreateMoleculeProjectUseCaseImpl {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        mut ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: num_bigint::BigInt,
    ) -> Result<serde_json::Value, CreateMoleculeProjectError> {
        // Resolve projects snapshot with Write privileges
        let (projects_dataset, df) = self
            .project_service
            .get_projects_snapshot(molecule_subject, DatasetAction::Write, true)
            .await
            .map_err(CreateMoleculeProjectError::from)?;

        use datafusion::prelude::*;

        // Normalize symbol to lowercase
        ipnft_symbol.make_ascii_lowercase();
        let lowercase_ipnft_symbol = ipnft_symbol;

        // Check for conflicts
        if let Some(df) = df {
            let df = df
                .filter(
                    col("ipnft_uid")
                        .eq(lit(&ipnft_uid))
                        .or(lower(col("ipnft_symbol")).eq(lit(&lowercase_ipnft_symbol))),
                )
                .int_err()?;

            let records = df.collect_json_aos().await.int_err()?;
            if let Some(record) = records.into_iter().next() {
                return Err(CreateMoleculeProjectError::Conflict { project: record });
            }
        }

        // Create a project account
        let molecule_account = self
            .account_service
            .try_get_account_by_id(&molecule_subject.account_id)
            .await?
            .unwrap();

        let project_account_name: odf::AccountName =
            format!("{}.{lowercase_ipnft_symbol}", molecule_account.account_name)
                .parse()
                .int_err()?;

        let project_email = format!("support+{project_account_name}@kamu.dev")
            .parse()
            .unwrap();

        // TODO: Remove tolerance to accounts that already exist after we have account
        // deletion api? Reusing existing accounts may be a security threat via
        // name squatting.
        let project_account = if let Some(acc) = self
            .account_service
            .account_by_name(&project_account_name)
            .await
            .int_err()?
        {
            acc
        } else {
            // TODO: Set avatar and display name?
            // https://avatars.githubusercontent.com/u/37688345?s=200&amp;v=4
            self.create_account_use_case
                .execute_derived(
                    &molecule_account,
                    &project_account_name,
                    CreateAccountUseCaseOptions::builder()
                        .maybe_email(Some(project_email))
                        .build(),
                )
                .await
                .int_err()?
        };

        // Create `data-room` dataset
        let snapshot = MoleculeDatasetSnapshots::data_room_v2(odf::DatasetAlias::new(
            Some(project_account_name.clone()),
            odf::DatasetName::new_unchecked("data-room"),
        ));
        let data_room_create_res = self
            .create_dataset_from_snapshot_use_case
            .execute(
                snapshot,
                CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Create `announcements` dataset
        let snapshot = MoleculeDatasetSnapshots::announcements(odf::DatasetAlias::new(
            Some(project_account_name.clone()),
            odf::DatasetName::new_unchecked("announcements"),
        ));
        let announcements_create_res = self
            .create_dataset_from_snapshot_use_case
            .execute(
                snapshot,
                CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Give maintainer permissions to molecule
        self.rebac_service
            .set_account_dataset_relation(
                &molecule_subject.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &data_room_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        self.rebac_service
            .set_account_dataset_relation(
                &molecule_subject.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &announcements_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        // Add project entry
        let now = chrono::Utc::now();
        let project_json = serde_json::json!({
            "account_id": project_account.id,
            "system_time": now,
            "event_time": now,
            "ipnft_symbol": lowercase_ipnft_symbol,
            "ipnft_address": ipnft_address,
            "ipnft_token_id": ipnft_token_id.to_string(),
            "ipnft_uid": ipnft_uid,
            "data_room_dataset_id": data_room_create_res.dataset_handle.id,
            "announcements_dataset_id": announcements_create_res.dataset_handle.id,
        });

        let mut project_internal_json = project_json.clone();
        project_internal_json["op"] = u8::from(odf::metadata::OperationType::Append).into();
        let project_internal_bytes =
            bytes::Bytes::from_owner(project_internal_json.to_string().into_bytes());

        self.push_ingest_use_case
            .execute(
                projects_dataset,
                kamu_core::DataSource::Buffer(project_internal_bytes),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(file_utils::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        Ok(project_json)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

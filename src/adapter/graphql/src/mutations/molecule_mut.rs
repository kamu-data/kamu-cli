// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::DatasetRegistryExt;

use crate::prelude::*;
use crate::queries::{Molecule, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MoleculeMut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl MoleculeMut {
    #[graphql(guard = "LoggedInGuard::new()")]
    #[tracing::instrument(level = "info", name = MoleculeMut_create_project, skip_all, fields(?ipt_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_address: String,
        ipnft_token_id: usize,
        ipnft_uid: String,
        ipt_symbol: String,
        ipt_address: String,
    ) -> Result<CreateProjectResult> {
        use datafusion::logical_expr::{col, lit};

        let (
            subject,
            dataset_reg,
            query_svc,
            account_repo,
            login_pass_provider,
            create_dataset_use_case,
            rebac_svc,
            push_ingest_use_case,
        ) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn domain::DatasetRegistry,
            dyn domain::QueryService,
            dyn kamu_accounts::AccountRepository,
            kamu_accounts_services::LoginPasswordAuthProvider,
            dyn kamu_datasets::CreateDatasetFromSnapshotUseCase,
            dyn kamu_auth_rebac::RebacService,
            dyn domain::PushIngestDataUseCase
        );

        // Check auth
        let subject_molecule = match subject.as_ref() {
            CurrentAccountSubject::Logged(subj) if subj.account_name == "molecule" => subj,
            _ => {
                return Err(GqlError::Access(odf::AccessError::Unauthorized(
                    "Only 'molecule' account can provision new projects".into(),
                )))
            }
        };

        if ipnft_uid != format!("{ipnft_address}_{ipnft_token_id}") {
            return Err(Error::new("Inconsistent ipnft info").into());
        }

        // Resolve projects dataset
        let projects_dataset = dataset_reg
            .get_dataset_by_ref(&"molecule/projects".parse().unwrap())
            .await
            .int_err()?;

        // Check for conflicts
        let query_res = query_svc
            .get_data(
                &projects_dataset.get_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        if let Some(df) = query_res.df {
            let df = df
                .filter(
                    col("ipnft_uid")
                        .eq(lit(&ipnft_uid))
                        .or(col("ipt_address").eq(lit(&ipt_address)))
                        .or(col("ipt_symbol").eq(lit(&ipt_symbol))),
                )
                .int_err()?;

            let df = odf::utils::data::changelog::project(
                df,
                &["account_id".to_string()],
                &odf::metadata::DatasetVocabulary::default(),
            )
            .int_err()?;

            let records = df.collect_json_aos().await.int_err()?;
            if let Some(record) = records.into_iter().next() {
                let project = MoleculeProject::from_json(record);
                return Ok(CreateProjectResult::Conflict(CreateProjectErrorConflict {
                    project,
                }));
            }
        }

        // Create account
        // TODO: Preserve PK
        let (_key, project_account_id) = odf::AccountID::new_generated_ed25519();
        let project_account_name: odf::AccountName =
            format!("molecule.{ipt_symbol}").parse().int_err()?;

        let project_account = kamu_accounts::Account {
            id: project_account_id,
            account_name: project_account_name.clone(),
            email: format!("support+{project_account_name}@kamu.dev")
                .parse()
                .unwrap(),
            display_name: project_account_name.to_string(),
            account_type: kamu_accounts::AccountType::Organization,
            avatar_url: Some(
                "https://avatars.githubusercontent.com/u/37688345?s=200&amp;v=4".into(),
            ),
            registered_at: chrono::Utc::now(),
            provider: kamu_accounts::PROVIDER_PASSWORD.into(),
            provider_identity_key: project_account_name.to_string(),
        };

        account_repo
            .create_account(&project_account)
            .await
            .int_err()?;

        // TODO: Generate random password
        login_pass_provider
            .save_password(&project_account.account_name, "molecule".into())
            .await?;

        // Create `data-room` dataset
        let snapshot = Molecule::dataset_snapshot_data_room(odf::DatasetAlias::new(
            Some(project_account_name.clone()),
            odf::DatasetName::new_unchecked("data-room"),
        ));
        let data_room_create_res = create_dataset_use_case
            .execute(
                snapshot,
                kamu_datasets::CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Create `announcements` dataset
        let snapshot = Molecule::dataset_snapshot_announcements(odf::DatasetAlias::new(
            Some(project_account_name.clone()),
            odf::DatasetName::new_unchecked("announcements"),
        ));
        let announcements_create_res = create_dataset_use_case
            .execute(
                snapshot,
                kamu_datasets::CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Give maintainer permissions to molecule
        rebac_svc
            .set_account_dataset_relation(
                &subject_molecule.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &data_room_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        rebac_svc
            .set_account_dataset_relation(
                &subject_molecule.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &announcements_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        // Add project entry
        let now = chrono::Utc::now();
        let project = MoleculeProject {
            account_id: project_account.id,
            system_time: now,
            event_time: now,
            ipnft_address,
            ipnft_token_id,
            ipnft_uid,
            ipt_address,
            ipt_symbol,
            data_room_dataset_id: data_room_create_res.dataset_handle.id,
            announcements_dataset_id: announcements_create_res.dataset_handle.id,
        };

        push_ingest_use_case
            .execute(
                &projects_dataset,
                kamu_core::DataSource::Buffer(
                    project.to_bytes(odf::metadata::OperationType::Append),
                ),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(kamu_core::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        Ok(CreateProjectResult::Success(CreateProjectSuccess {
            project,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateProjectResult {
    Success(CreateProjectSuccess),
    Conflict(CreateProjectErrorConflict),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateProjectSuccess {
    pub project: MoleculeProject,
}
#[ComplexObject]
impl CreateProjectSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateProjectErrorConflict {
    project: MoleculeProject,
}
#[ComplexObject]
impl CreateProjectErrorConflict {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        format!(
            "Conflict with existing project {} ({})",
            self.project.ipnft_uid, self.project.ipt_symbol,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

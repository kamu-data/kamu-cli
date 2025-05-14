// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;
use kamu_accounts::{AccountServiceExt as _, CurrentAccountSubject};
use kamu_core::DatasetRegistryExt;

use crate::prelude::*;
use crate::queries::{Molecule, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MoleculeMut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl MoleculeMut {
    #[graphql(guard = "LoggedInGuard::new()")]
    #[tracing::instrument(level = "info", name = MoleculeMut_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: usize,
    ) -> Result<CreateProjectResult> {
        use datafusion::logical_expr::{col, lit};

        let (
            subject,
            query_svc,
            account_svc,
            create_account_use_case,
            create_dataset_use_case,
            rebac_svc,
            push_ingest_use_case,
        ) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn domain::QueryService,
            dyn kamu_accounts::AccountService,
            dyn kamu_accounts::CreateAccountUseCase,
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
        let projects_dataset = Molecule::get_projects_dataset(ctx, true).await?;

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
                        .or(col("ipnft_symbol").eq(lit(&ipnft_symbol))),
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

        // Create project account
        let molecule_account = account_svc
            .account_by_id(subject.account_id())
            .await?
            .unwrap();

        let project_account_name: odf::AccountName =
            format!("molecule.{ipnft_symbol}").parse().int_err()?;

        let project_email = format!("support+{project_account_name}@kamu.dev")
            .parse()
            .unwrap();

        // TODO: Remove tolerance to accounts that already exist after we have account
        // deletion api? Reusing existing accounts may be a security threat via
        // name squatting.
        let project_account = if let Some(acc) = account_svc
            .account_by_name(&project_account_name)
            .await
            .int_err()?
        {
            acc
        } else {
            // TODO: Set avatar and display name?
            // https://avatars.githubusercontent.com/u/37688345?s=200&amp;v=4
            create_account_use_case
                .execute(
                    &molecule_account,
                    &project_account_name,
                    Some(project_email),
                )
                .await
                .int_err()?
        };

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
            ipnft_symbol,
            ipnft_address,
            ipnft_token_id,
            ipnft_uid,
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

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMut_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMut>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // Resolve projects dataset
        let projects_dataset = Molecule::get_projects_dataset(ctx, false).await?;

        // Query data
        let query_res = query_svc
            .get_data(
                &projects_dataset.get_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;

        let df = odf::utils::data::changelog::project(
            df,
            &["account_id".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let entry = MoleculeProjectMut::from_json(records.into_iter().next().unwrap());

        Ok(Some(entry))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(dead_code)]
pub struct MoleculeProjectMut {
    pub account_id: odf::AccountID,
    pub data_room_dataset_id: odf::DatasetID,
    pub announcements_dataset_id: odf::DatasetID,
}

impl MoleculeProjectMut {
    pub fn from_json(record: serde_json::Value) -> Self {
        let serde_json::Value::Object(record) = record else {
            unreachable!()
        };

        let account_id =
            odf::AccountID::from_did_str(record["account_id"].as_str().unwrap()).unwrap();

        let data_room_dataset_id =
            odf::DatasetID::from_did_str(record["data_room_dataset_id"].as_str().unwrap()).unwrap();

        let announcements_dataset_id =
            odf::DatasetID::from_did_str(record["announcements_dataset_id"].as_str().unwrap())
                .unwrap();

        Self {
            account_id,
            data_room_dataset_id,
            announcements_dataset_id,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl MoleculeProjectMut {
    /// Creates an announcement record for the project
    #[tracing::instrument(level = "info", name = MoleculeProjectMut_create_announcement, skip_all)]
    async fn create_announcement(
        &self,
        ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<String>>,
        molecule_access_level: String,
        molecule_change_by: String,
    ) -> Result<CreateAnnouncementResult> {
        let (dataset_reg, push_ingest_use_case) = from_catalog_n!(
            ctx,
            dyn domain::DatasetRegistry,
            dyn domain::PushIngestDataUseCase
        );

        // Validate attachment links
        let attachments = attachments.unwrap_or_default();
        for att in &attachments {
            let did = match odf::DatasetID::from_did_str(att) {
                Ok(did) => did,
                Err(err) => {
                    return Ok(CreateAnnouncementResult::InvalidAttachment(
                        CreateAnnouncementErrorInvalidAttachment {
                            message: err.to_string(),
                        },
                    ))
                }
            };

            if dataset_reg
                .try_resolve_dataset_handle_by_ref(&did.as_local_ref())
                .await?
                .is_none()
            {
                return Ok(CreateAnnouncementResult::InvalidAttachment(
                    CreateAnnouncementErrorInvalidAttachment {
                        message: format!("Dataset {did} not found"),
                    },
                ));
            }
        }

        let dataset = dataset_reg
            .get_dataset_by_id(&self.announcements_dataset_id)
            .await
            .int_err()?;

        let announcement_id = uuid::Uuid::new_v4();

        let record = serde_json::json!({
            "op": u8::from(odf::metadata::OperationType::Append),
            "announcement_id": announcement_id.to_string(),
            "headline": headline,
            "body": body,
            "attachments": attachments,
            "molecule_access_level": molecule_access_level,
            "molecule_change_by": molecule_change_by,
        });

        push_ingest_use_case
            .execute(
                &dataset,
                kamu_core::DataSource::Buffer(bytes::Bytes::from_owner(
                    record.to_string().into_bytes(),
                )),
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

        Ok(CreateAnnouncementResult::Success(
            CreateAnnouncementSuccess {
                announcement_id: announcement_id.to_string(),
            },
        ))
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
            self.project.ipnft_symbol, self.project.ipnft_uid,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateAnnouncementResult {
    Success(CreateAnnouncementSuccess),
    InvalidAttachment(CreateAnnouncementErrorInvalidAttachment),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementSuccess {
    /// ID of the newly-created announcement
    pub announcement_id: String,
}
#[ComplexObject]
impl CreateAnnouncementSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementErrorInvalidAttachment {
    message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorInvalidAttachment {
    async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

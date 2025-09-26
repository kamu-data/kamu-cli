// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu::domain;
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::auth::DatasetAction;
use kamu_core::{DatasetRegistryExt, ResolvedDataset};
use odf::utils::data::DataFrameExt;

use super::{CollectionEntry, VersionedFileEntry};
use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Molecule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Molecule {
    pub fn dataset_snapshot_projects(alias: odf::DatasetAlias) -> odf::metadata::DatasetSnapshot {
        odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: "default".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(
                            [
                                "op INT",
                                "account_id STRING",
                                "ipnft_symbol STRING",
                                "ipnft_uid STRING",
                                "ipnft_address STRING",
                                "ipnft_token_id STRING",
                                "data_room_dataset_id STRING",
                                "announcements_dataset_id STRING",
                            ]
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        ),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyChangelogStream {
                        primary_key: vec!["account_id".to_string()],
                    }
                    .into(),
                }
                .into(),
                odf::metadata::SetInfo {
                    description: Some("List of projects tracked by Molecule.xyz".into()),
                    keywords: Some(vec![
                        "DeSci".to_string(),
                        "BioTech".to_string(),
                        "Funding".to_string(),
                        "Crypto".to_string(),
                    ]),
                }
                .into(),
                odf::metadata::SetAttachments {
                    attachments: odf::metadata::AttachmentsEmbedded {
                        items: vec![odf::metadata::AttachmentEmbedded {
                            path: "README.md".into(),
                            content: indoc::indoc!(
                                r#"
                                # Projects tracked by Molecule.xyz

                                Molecule is a decentralized biotech protocol,
                                building a web3 marketplace for research-related IP.
                                Our platform and scalable framework for biotech DAOs
                                connects academics and biotech companies with quick and
                                easy funding, while enabling patient, researcher, and funder
                                communities to directly govern and own research-related IP.

                                Find out more at https://molecule.xyz/
                                "#
                            )
                            .into(),
                        }],
                    }
                    .into(),
                }
                .into(),
            ],
        }
    }

    pub fn dataset_snapshot_data_room(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        super::Collection::dataset_snapshot(
            alias,
            vec![ColumnInput {
                name: "molecule_change_by".into(),
                data_type: DataTypeInput {
                    ddl: "STRING".into(),
                },
            }],
            vec![],
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn dataset_snapshot_announcements(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: "default".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(
                            [
                                "op INT",
                                "announcement_id STRING",
                                "headline STRING",
                                "body STRING",
                                "attachments Array<STRING>",
                                "molecule_access_level STRING",
                                "molecule_change_by STRING",
                            ]
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        ),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyChangelogStream {
                        primary_key: vec!["announcement_id".to_string()],
                    }
                    .into(),
                }
                .into(),
                odf::metadata::SetInfo {
                    description: Some("Project announcements".into()),
                    keywords: Some(vec![
                        "DeSci".to_string(),
                        "BioTech".to_string(),
                        "Funding".to_string(),
                        "Crypto".to_string(),
                    ]),
                }
                .into(),
                odf::metadata::SetAttachments {
                    attachments: odf::metadata::AttachmentsEmbedded {
                        items: vec![odf::metadata::AttachmentEmbedded {
                            path: "README.md".into(),
                            content: indoc::indoc!(
                                r#"
                                # Project announcements

                                TODO
                                "#
                            )
                            .into(),
                        }],
                    }
                    .into(),
                }
                .into(),
            ],
        }
    }

    pub async fn get_projects_dataset(
        ctx: &Context<'_>,
        molecule_account_name: &odf::AccountName,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<domain::ResolvedDataset> {
        let dataset_reg = from_catalog_n!(ctx, dyn RebacDatasetRegistryFacade);

        const PROJECTS_DATASET_NAME: &str = "projects";

        let projects_dataset_alias = odf::DatasetAlias::new(
            Some(molecule_account_name.clone()),
            odf::DatasetName::new_unchecked(PROJECTS_DATASET_NAME),
        );

        match dataset_reg
            .resolve_dataset_by_ref(&projects_dataset_alias.as_local_ref(), action)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let create_dataset_use_case =
                    from_catalog_n!(ctx, dyn kamu_datasets::CreateDatasetFromSnapshotUseCase);

                let snapshot = Self::dataset_snapshot_projects(odf::DatasetAlias::new(
                    None,
                    odf::DatasetName::new_unchecked(PROJECTS_DATASET_NAME),
                ));

                let create_res = create_dataset_use_case
                    .execute(
                        snapshot,
                        kamu_datasets::CreateDatasetUseCaseOptions {
                            dataset_visibility: odf::DatasetVisibility::Private,
                        },
                    )
                    .await
                    .int_err()?;

                // TODO: Use case should return ResolvedDataset directly
                Ok(ResolvedDataset::new(
                    create_res.dataset,
                    create_res.dataset_handle,
                ))
            }
            Err(RebacDatasetRefUnresolvedError::NotFound(err)) => Err(GqlError::Gql(err.into())),
            Err(RebacDatasetRefUnresolvedError::Access(err)) => Err(GqlError::Access(err)),
            Err(err) => Err(err.int_err().into()),
        }
    }

    pub async fn get_projects_snapshot(
        ctx: &Context<'_>,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>)> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let subject_molecule = molecule_subject(ctx)?;

        // Resolve projects dataset
        let projects_dataset = Self::get_projects_dataset(
            ctx,
            &subject_molecule.account_name,
            action,
            create_if_not_exist,
        )
        .await?;

        // Query full data
        let df = match query_svc
            .get_data(
                &projects_dataset.get_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
        {
            Ok(res) => Ok(res.df),
            Err(domain::QueryError::Access(err)) => Err(GqlError::Access(err)),
            Err(err) => Err(err.int_err().into()),
        }?;

        // Project into snapshot
        let df = if let Some(df) = df {
            Some(
                odf::utils::data::changelog::project(
                    df,
                    &["account_id".to_string()],
                    &odf::metadata::DatasetVocabulary::default(),
                )
                .int_err()?,
            )
        } else {
            None
        };

        Ok((projects_dataset, df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Molecule {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = Molecule_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProject>> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = Self::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(None);
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let entry = MoleculeProject::from_json(records.into_iter().next().unwrap());

        Ok(Some(entry))
    }

    /// List the registered projects
    #[tracing::instrument(level = "info", name = Molecule_projects, skip_all)]
    async fn projects(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectConnection> {
        use datafusion::logical_expr::col;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PROJECTS_PER_PAGE);

        let Some(df) = Self::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(MoleculeProjectConnection::new(Vec::new(), 0, per_page, 0));
        };

        let total_count = df.clone().count().await.int_err()?;
        let df = df
            .sort(vec![col("ipnft_symbol").sort(true, false)])
            .int_err()?
            .limit(page * per_page, Some(per_page))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let nodes = records
            .into_iter()
            .map(MoleculeProject::from_json)
            .collect();

        Ok(MoleculeProjectConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = Molecule_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectEventConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: PERF: This "brute force" approach will not scale with growth of
        // projects and has to be revisited
        let Some(df) = Self::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(MoleculeProjectEventConnection::new(Vec::new(), 0, per_page));
        };

        let projects_by_announcement_dataset: std::collections::BTreeMap<
            odf::DatasetID,
            Arc<MoleculeProject>,
        > = df
            .collect_json_aos()
            .await
            .int_err()?
            .into_iter()
            .map(MoleculeProject::from_json)
            .map(|p| (p.announcements_dataset_id.clone(), Arc::new(p)))
            .collect();

        let announcement_dataset_refs: Vec<odf::DatasetRef> = projects_by_announcement_dataset
            .keys()
            .map(odf::DatasetID::as_local_ref)
            .collect();

        let mut announcement_dataframes = Vec::new();
        const DATASET_ID_COL: &str = "__dataset_id__";

        for resp in query_svc
            .get_data_multi(&announcement_dataset_refs, true)
            .await
            .int_err()?
        {
            let Some(df) = resp.df else {
                // Skip empty datasets
                continue;
            };

            // Attach dataset ID as a column to records to associate them later
            let df = df
                .with_column(DATASET_ID_COL, lit(resp.dataset_handle.id.to_string()))
                .int_err()?;

            announcement_dataframes.push(df);
        }

        let Some(df_global_announcements) =
            DataFrameExt::union_all(announcement_dataframes).int_err()?
        else {
            return Ok(MoleculeProjectEventConnection::new(Vec::new(), 0, per_page));
        };

        let vocab = odf::metadata::DatasetVocabulary::default();

        let df_global_announcements = df_global_announcements
            .sort(vec![col(&vocab.event_time_column).sort(false, false)])
            .int_err()?
            .limit(per_page * page, Some(per_page))
            .int_err()?;

        let nodes = df_global_announcements
            .collect_json_aos()
            .await
            .int_err()?
            .into_iter()
            .map(|mut record| {
                let obj = record.as_object_mut().unwrap();
                obj.remove(&vocab.offset_column);
                obj.remove(&vocab.operation_type_column);

                let did = odf::DatasetID::from_did_str(
                    obj.remove(DATASET_ID_COL).unwrap().as_str().unwrap(),
                )
                .unwrap();

                MoleculeProjectEvent::Announcement(MoleculeProjectEventAnnouncement {
                    project: Arc::clone(&projects_by_announcement_dataset[&did]),
                    announcement: record,
                })
            })
            .collect();

        Ok(MoleculeProjectEventConnection::new(nodes, page, per_page))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct MoleculeProject {
    #[graphql(skip)]
    pub account_id: odf::AccountID,

    /// System time when this version was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this version was created/updated
    pub event_time: DateTime<Utc>,

    /// Symbolic name of the project
    pub ipnft_symbol: String,

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub ipnft_uid: String,

    /// Address of the IPNFT contract
    pub ipnft_address: String,

    // NOTE: For backward compatibility (and existing projects),
    //       we continue using BigInt type, which is wider than needed U256.
    /// Token ID withing the IPNFT contract
    pub ipnft_token_id: BigInt,

    #[graphql(skip)]
    pub data_room_dataset_id: odf::DatasetID,

    #[graphql(skip)]
    pub announcements_dataset_id: odf::DatasetID,
}

impl MoleculeProject {
    pub fn from_json(record: serde_json::Value) -> Self {
        let serde_json::Value::Object(mut record) = record else {
            unreachable!()
        };

        // Parse system columns
        let vocab = odf::metadata::DatasetVocabulary::default();

        record.remove(&vocab.offset_column);

        record.remove(&vocab.operation_type_column);

        let system_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.system_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();

        let event_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.event_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();

        // Parse core columns
        let account_id =
            odf::AccountID::from_did_str(record.remove("account_id").unwrap().as_str().unwrap())
                .unwrap();

        let ipnft_symbol = record
            .remove("ipnft_symbol")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipnft_uid = record
            .remove("ipnft_uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipnft_address = record
            .remove("ipnft_address")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipnft_token_id = {
            let value = record.remove("ipnft_token_id").unwrap();

            if let Some(s) = value.as_str() {
                BigInt::new(s.parse().unwrap())
            } else if let Some(n) = value.as_number() {
                BigInt::new(n.to_string().parse().unwrap())
            } else {
                panic!("Unexpected value for ipnft_token_id: {value:?}");
            }
        };

        let data_room_dataset_id = odf::DatasetID::from_did_str(
            record
                .remove("data_room_dataset_id")
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap();

        let announcements_dataset_id = odf::DatasetID::from_did_str(
            record
                .remove("announcements_dataset_id")
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap();

        Self {
            account_id,
            system_time,
            event_time,
            ipnft_symbol,
            ipnft_uid,
            ipnft_address,
            ipnft_token_id,
            data_room_dataset_id,
            announcements_dataset_id,
        }
    }

    pub fn to_record_data(&self) -> serde_json::Value {
        let mut r = serde_json::Value::Object(Default::default());
        r["account_id"] = self.account_id.to_string().into();
        r["ipnft_symbol"] = self.ipnft_symbol.clone().into();
        r["ipnft_uid"] = self.ipnft_uid.clone().into();
        r["ipnft_address"] = self.ipnft_address.clone().into();
        r["ipnft_token_id"] = self.ipnft_token_id.clone().into_inner().to_string().into();
        r["data_room_dataset_id"] = self.data_room_dataset_id.to_string().into();
        r["announcements_dataset_id"] = self.announcements_dataset_id.to_string().into();
        r
    }

    pub fn to_bytes(&self, op: odf::metadata::OperationType) -> bytes::Bytes {
        let mut record = self.to_record_data();
        record["op"] = u8::from(op).into();

        let buf = record.to_string().into_bytes();
        bytes::Bytes::from_owner(buf)
    }

    async fn get_activity_announcements(
        &self,
        ctx: &Context<'_>,
        project: &Arc<Self>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let df = match query_svc
            .tail(
                &self.announcements_dataset_id.as_local_ref(),
                0,
                limit as u64,
                domain::GetDataOptions::default(),
            )
            .await
        {
            Ok(res) => match res.df {
                Some(df) => df,
                None => return Ok(Vec::new()),
            },
            Err(domain::QueryError::Access(err)) => return Err(GqlError::Access(err)),
            Err(err) => return Err(err.int_err().into()),
        };

        let records = df.collect_json_aos().await.int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();

        let records = records
            .into_iter()
            .map(|mut record| {
                let obj = record.as_object_mut().unwrap();
                obj.remove(&vocab.offset_column);
                obj.remove(&vocab.operation_type_column);

                MoleculeProjectEvent::Announcement(MoleculeProjectEventAnnouncement {
                    project: Arc::clone(project),
                    announcement: record,
                })
            })
            .collect();

        Ok(records)
    }

    async fn get_activity_data_room(
        &self,
        ctx: &Context<'_>,
        project: &Arc<Self>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let df = match query_svc
            .tail(
                &self.data_room_dataset_id.as_local_ref(),
                0,
                limit as u64,
                domain::GetDataOptions::default(),
            )
            .await
        {
            Ok(res) => match res.df {
                Some(df) => df,
                None => return Ok(Vec::new()),
            },
            Err(domain::QueryError::Access(err)) => return Err(GqlError::Access(err)),
            Err(err) => return Err(err.int_err().into()),
        };

        let records = df.collect_json_aos().await.int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();

        let records = records
            .into_iter()
            .map(|record| {
                let op = record[&vocab.operation_type_column].as_i64().unwrap();
                let op = odf::metadata::OperationType::try_from(u8::try_from(op).unwrap()).unwrap();
                (op, record)
            })
            .filter(|(op, _)| *op != odf::metadata::OperationType::CorrectFrom)
            .map(|(op, record)| {
                let entry = CollectionEntry::from_json(record)?;

                let event = match op {
                    odf::metadata::OperationType::Append => {
                        MoleculeProjectEvent::DataRoomEntryAdded(
                            MoleculeProjectEventDataRoomEntryAdded {
                                project: Arc::clone(project),
                                entry,
                            },
                        )
                    }
                    odf::metadata::OperationType::Retract => {
                        MoleculeProjectEvent::DataRoomEntryRemoved(
                            MoleculeProjectEventDataRoomEntryRemoved {
                                project: Arc::clone(project),
                                entry,
                            },
                        )
                    }
                    odf::metadata::OperationType::CorrectFrom => unreachable!(),
                    odf::metadata::OperationType::CorrectTo => {
                        MoleculeProjectEvent::DataRoomEntryUpdated(
                            MoleculeProjectEventDataRoomEntryUpdated {
                                project: Arc::clone(project),
                                new_entry: entry,
                            },
                        )
                    }
                };

                Ok(event)
            })
            .collect::<Result<_, InternalError>>()?;

        Ok(records)
    }

    // TODO: PERF: This will get very expensive on large number of files and
    // versions - we likely need to create a derivative dataset to track activity,
    // so we could query it in a single go.
    async fn get_activity_files(
        &self,
        ctx: &Context<'_>,
        project: &Arc<Self>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let (query_svc, dataset_reg) =
            from_catalog_n!(ctx, dyn domain::QueryService, dyn domain::DatasetRegistry);

        let df = match query_svc
            .get_data(
                &self.data_room_dataset_id.as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
        {
            Ok(res) => match res.df {
                Some(df) => df,
                None => return Ok(Vec::new()),
            },
            Err(domain::QueryError::Access(err)) => return Err(GqlError::Access(err)),
            Err(err) => return Err(err.int_err().into()),
        };

        let records = df
            .select(vec![datafusion::prelude::col("ref")])
            .int_err()?
            .distinct()
            .int_err()?
            .collect_json_aos()
            .await
            .int_err()?;

        let dataset_ids: Vec<odf::DatasetID> = records
            .into_iter()
            .map(|r| odf::DatasetID::from_did_str(r["ref"].as_str().unwrap()).unwrap())
            .collect();

        let mut events = Vec::new();

        for did in dataset_ids {
            let resolved_dataset = dataset_reg
                .get_dataset_by_ref(&did.as_local_ref())
                .await
                .int_err()?;

            let df = match query_svc
                .tail(
                    &resolved_dataset.get_handle().as_local_ref(),
                    0,
                    limit as u64,
                    domain::GetDataOptions::default(),
                )
                .await
            {
                Ok(res) => match res.df {
                    Some(df) => df,
                    None => continue,
                },
                // Skip datasets we don't have access to
                Err(domain::QueryError::Access(_)) => continue,
                Err(err) => return Err(err.int_err().into()),
            };

            let records = df.collect_json_aos().await.int_err()?;

            let project_account = Account::from_account_id(ctx, self.account_id.clone()).await?;

            // TODO: Assuming every collection entry is a versioned file
            for record in records {
                let dataset = Dataset::new_access_checked(
                    project_account.clone(),
                    resolved_dataset.get_handle().clone(),
                );

                let entry = VersionedFileEntry::from_json(resolved_dataset.clone(), record)?;

                events.push(MoleculeProjectEvent::FileUpdated(
                    MoleculeProjectEventFileUpdated {
                        project: Arc::clone(project),
                        dataset,
                        new_entry: entry,
                    },
                ));
            }
        }

        Ok(events)
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[ComplexObject]
impl MoleculeProject {
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Project's organizational account
    #[tracing::instrument(level = "info", name = MoleculeProject_account, skip_all)]
    async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.account_id.clone()).await?;
        Ok(account)
    }

    /// Project's data room dataset
    #[tracing::instrument(level = "info", name = MoleculeProject_data_room, skip_all)]
    async fn data_room(&self, ctx: &Context<'_>) -> Result<Dataset> {
        match Dataset::try_from_ref(ctx, &self.data_room_dataset_id.as_local_ref()).await? {
            TransformInputDataset::Accessible(ds) => Ok(ds.dataset),
            TransformInputDataset::NotAccessible(_) => Err(GqlError::Access(
                odf::AccessError::Unauthorized("Dataset inaccessible".into()),
            )),
        }
    }

    /// Project's announcements dataset
    #[tracing::instrument(level = "info", name = MoleculeProject_announcements, skip_all)]
    async fn announcements(&self, ctx: &Context<'_>) -> Result<Dataset> {
        match Dataset::try_from_ref(ctx, &self.announcements_dataset_id.as_local_ref()).await? {
            TransformInputDataset::Accessible(ds) => Ok(ds.dataset),
            TransformInputDataset::NotAccessible(_) => Err(GqlError::Access(
                odf::AccessError::Unauthorized("Dataset inaccessible".into()),
            )),
        }
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProject_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectEventConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        // We fetch up to limit from all sources, then combine them and sort in reverse
        // chronological order, and finally truncate to the requested page size
        let limit = per_page * (page + 1);

        let project = Arc::new(self.clone());

        let mut events = Vec::new();
        events.append(
            &mut self
                .get_activity_announcements(ctx, &project, limit)
                .await?,
        );
        events.append(&mut self.get_activity_data_room(ctx, &project, limit).await?);
        events.append(&mut self.get_activity_files(ctx, &project, limit).await?);

        let mut events_timed = Vec::with_capacity(events.len());
        for e in events {
            let system_time = e.system_time(ctx).await?;
            events_timed.push((system_time, e));
        }

        events_timed.sort_by(|a, b| a.0.cmp(&b.0).reverse());

        let nodes = events_timed
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|(_t, e)| e)
            .collect();

        Ok(MoleculeProjectEventConnection::new(nodes, page, per_page))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MoleculeProject,
    MoleculeProjectConnection,
    MoleculeProjectEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "project", ty = "&Arc<MoleculeProject>"))]
#[graphql(field(name = "system_time", ty = "DateTime<Utc>"))]
pub enum MoleculeProjectEvent {
    DataRoomEntryAdded(MoleculeProjectEventDataRoomEntryAdded),
    DataRoomEntryRemoved(MoleculeProjectEventDataRoomEntryRemoved),
    DataRoomEntryUpdated(MoleculeProjectEventDataRoomEntryUpdated),
    Announcement(MoleculeProjectEventAnnouncement),
    FileUpdated(MoleculeProjectEventFileUpdated),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProjectEventDataRoomEntryAdded {
    /// Associated project
    pub project: Arc<MoleculeProject>,
    /// Collection entry
    pub entry: CollectionEntry,
}
#[ComplexObject]
impl MoleculeProjectEventDataRoomEntryAdded {
    async fn system_time(&self) -> DateTime<Utc> {
        self.entry.system_time
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProjectEventDataRoomEntryRemoved {
    /// Associated project
    pub project: Arc<MoleculeProject>,
    /// Collection entry
    pub entry: CollectionEntry,
}
#[ComplexObject]
impl MoleculeProjectEventDataRoomEntryRemoved {
    async fn system_time(&self) -> DateTime<Utc> {
        self.entry.system_time
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProjectEventDataRoomEntryUpdated {
    /// Associated project
    pub project: Arc<MoleculeProject>,
    /// Collection entry
    pub new_entry: CollectionEntry,
}
#[ComplexObject]
impl MoleculeProjectEventDataRoomEntryUpdated {
    async fn system_time(&self) -> DateTime<Utc> {
        self.new_entry.system_time
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProjectEventAnnouncement {
    /// Associated project
    pub project: Arc<MoleculeProject>,
    /// Announcement record
    pub announcement: serde_json::Value,
}
#[ComplexObject]
impl MoleculeProjectEventAnnouncement {
    async fn system_time(&self) -> DateTime<Utc> {
        let vocab = odf::metadata::DatasetVocabulary::default();

        DateTime::parse_from_rfc3339(
            self.announcement[&vocab.event_time_column]
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProjectEventFileUpdated {
    /// Associated project
    pub project: Arc<MoleculeProject>,

    /// Versioned file dataset
    pub dataset: Dataset,

    /// New file version entry
    pub new_entry: VersionedFileEntry,
}
#[ComplexObject]
impl MoleculeProjectEventFileUpdated {
    async fn system_time(&self) -> DateTime<Utc> {
        self.new_entry.system_time
    }
}

page_based_stream_connection!(
    MoleculeProjectEvent,
    MoleculeProjectEventConnection,
    MoleculeProjectEventEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_ORG_ACCOUNTS: [&str; 2] = ["molecule", "molecule.dev"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn molecule_subject(ctx: &Context<'_>) -> Result<LoggedAccount> {
    // Check auth
    let subject = from_catalog_n!(ctx, CurrentAccountSubject);
    let subject_molecule = match subject.as_ref() {
        CurrentAccountSubject::Logged(subj)
            if MOLECULE_ORG_ACCOUNTS.contains(&subj.account_name.as_str()) =>
        {
            subj
        }
        _ => {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                format!(
                    "Only accounts {} can provision projects",
                    MOLECULE_ORG_ACCOUNTS
                        .iter()
                        .map(|account_name| format!("'{account_name}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .as_str()
                .into(),
            )));
        }
    };
    Ok(subject_molecule.clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

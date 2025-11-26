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
use database_common::PaginationOpts;
use kamu::domain;
use kamu_molecule_domain::{
    MoleculeFindProjectError,
    MoleculeFindProjectUseCase,
    MoleculeProjectListing,
    MoleculeViewProjectsError,
    MoleculeViewProjectsUseCase,
};
use odf::utils::data::DataFrameExt;

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::{Account, CollectionEntry, Dataset, VersionedFileEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct MoleculeV1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeV1 {
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

    async fn get_molecule_projects_listing(
        &self,
        ctx: &Context<'_>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectListing> {
        let molecule_subject = molecule_subject(ctx)?;

        let molecule_view_projects = from_catalog_n!(ctx, dyn MoleculeViewProjectsUseCase);
        let listing = molecule_view_projects
            .execute(&molecule_subject, pagination)
            .await
            .map_err(|e| match e {
                MoleculeViewProjectsError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeViewProjectsError::Access(e) => GqlError::Access(e),
                e @ MoleculeViewProjectsError::Internal(_) => e.int_err().into(),
            })?;

        Ok(listing)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeV1 {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV1_project, skip_all, fields(?ipnft_uid))]
    pub async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProject>> {
        let molecule_subject = molecule_subject(ctx)?;

        let molecule_find_project = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);
        let maybe_project = molecule_find_project
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                MoleculeFindProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeFindProjectError::Access(e) => GqlError::Access(e),
                e @ MoleculeFindProjectError::Internal(_) => e.int_err().into(),
            })?
            .map(MoleculeProject::new);

        Ok(maybe_project)
    }

    /// List the registered projects
    #[tracing::instrument(level = "info", name = MoleculeV1_projects, skip_all)]
    pub async fn projects(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PROJECTS_PER_PAGE);

        let listing = self
            .get_molecule_projects_listing(
                ctx,
                Some(PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                }),
            )
            .await?;

        let nodes = listing
            .projects
            .into_iter()
            .map(MoleculeProject::new)
            .collect();

        Ok(MoleculeProjectConnection::new(
            nodes,
            page,
            per_page,
            listing.total_count,
        ))
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = MoleculeV1_activity, skip_all)]
    pub async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectEventConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        let query_dataset_data = from_catalog_n!(ctx, dyn domain::QueryDatasetDataUseCase);

        // TODO: PERF: This "brute force" approach will not scale with growth of
        // projects and has to be revisited
        let projects_listing = self
            .get_molecule_projects_listing(ctx, None /* unbounded */)
            .await?;
        if projects_listing.total_count == 0 {
            return Ok(MoleculeProjectEventConnection::new(Vec::new(), 0, per_page));
        }

        let projects_by_announcement_dataset: std::collections::BTreeMap<
            odf::DatasetID,
            Arc<MoleculeProject>,
        > = projects_listing
            .projects
            .into_iter()
            .map(MoleculeProject::new)
            .map(|p| (p.entity.announcements_dataset_id.clone(), Arc::new(p)))
            .collect();

        let announcement_dataset_refs: Vec<odf::DatasetRef> = projects_by_announcement_dataset
            .keys()
            .map(odf::DatasetID::as_local_ref)
            .collect();

        let mut announcement_dataframes = Vec::new();
        const DATASET_ID_COL: &str = "__dataset_id__";

        for resp in query_dataset_data
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
                .with_column(DATASET_ID_COL, lit(resp.source.get_id().to_string()))
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

#[derive(Clone)]
pub struct MoleculeProject {
    pub(crate) entity: kamu_molecule_domain::MoleculeProjectEntity,
}

impl MoleculeProject {
    pub fn new(entity: kamu_molecule_domain::MoleculeProjectEntity) -> Self {
        Self { entity }
    }

    async fn get_activity_announcements(
        self: &Arc<Self>,
        ctx: &Context<'_>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let query_dataset_data = from_catalog_n!(ctx, dyn domain::QueryDatasetDataUseCase);

        let df = match query_dataset_data
            .tail(
                &self.entity.announcements_dataset_id.as_local_ref(),
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
                    project: Arc::clone(self),
                    announcement: record,
                })
            })
            .collect();

        Ok(records)
    }

    async fn get_activity_data_room(
        self: &Arc<Self>,
        ctx: &Context<'_>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let query_dataset_data = from_catalog_n!(ctx, dyn domain::QueryDatasetDataUseCase);

        let df = match query_dataset_data
            .tail(
                &self.entity.data_room_dataset_id.as_local_ref(),
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
                                project: Arc::clone(self),
                                entry,
                            },
                        )
                    }
                    odf::metadata::OperationType::Retract => {
                        MoleculeProjectEvent::DataRoomEntryRemoved(
                            MoleculeProjectEventDataRoomEntryRemoved {
                                project: Arc::clone(self),
                                entry,
                            },
                        )
                    }
                    odf::metadata::OperationType::CorrectFrom => unreachable!(),
                    odf::metadata::OperationType::CorrectTo => {
                        MoleculeProjectEvent::DataRoomEntryUpdated(
                            MoleculeProjectEventDataRoomEntryUpdated {
                                project: Arc::clone(self),
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
        self: &Arc<Self>,
        ctx: &Context<'_>,
        limit: usize,
    ) -> Result<Vec<MoleculeProjectEvent>> {
        let query_dataset_data = from_catalog_n!(ctx, dyn domain::QueryDatasetDataUseCase);

        let df = match query_dataset_data
            .get_data(
                &self.entity.data_room_dataset_id.as_local_ref(),
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
            let (df, source) = match query_dataset_data
                .tail(
                    &did.as_local_ref(),
                    0,
                    limit as u64,
                    domain::GetDataOptions::default(),
                )
                .await
            {
                Ok(res) => match res.df {
                    Some(df) => (df, res.source),
                    None => continue,
                },
                // Skip datasets we don't have access to
                Err(domain::QueryError::Access(_)) => continue,
                Err(err) => return Err(err.int_err().into()),
            };

            let records = df.collect_json_aos().await.int_err()?;

            let project_account =
                Account::from_account_id(ctx, self.entity.account_id.clone()).await?;

            // TODO: Assuming every collection entry is a versioned file
            for record in records {
                let dataset =
                    Dataset::from_resolved_authorized_dataset(project_account.clone(), &source);

                let entry = VersionedFileEntry::from_json(source.clone(), record)?;

                events.push(MoleculeProjectEvent::FileUpdated(
                    MoleculeProjectEventFileUpdated {
                        project: Arc::clone(self),
                        dataset,
                        new_entry: entry,
                    },
                ));
            }
        }

        Ok(events)
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProject {
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// System time when this project was created/updated
    pub async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    /// Event time when this project was created/updated
    pub async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    /// Symbolic name of the project
    pub async fn ipnft_symbol(&self) -> &str {
        &self.entity.ipnft_symbol
    }

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub async fn ipnft_uid(&self) -> &str {
        &self.entity.ipnft_uid
    }

    /// Address of the IPNFT contract
    pub async fn ipnft_address(&self) -> &str {
        &self.entity.ipnft_address
    }

    // NOTE: For backward compatibility (and existing projects),
    //       we continue using BigInt type, which is wider than needed U256.

    /// Token ID withing the IPNFT contract
    pub async fn ipnft_token_id(&self) -> BigInt {
        BigInt::new(self.entity.ipnft_token_id.clone())
    }

    /// Project's organizational account
    #[tracing::instrument(level = "info", name = MoleculeProject_account, skip_all)]
    pub async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.entity.account_id.clone()).await?;
        Ok(account)
    }

    /// Project's data room dataset
    #[tracing::instrument(level = "info", name = MoleculeProject_data_room, skip_all)]
    pub async fn data_room(&self, ctx: &Context<'_>) -> Result<Dataset> {
        match Dataset::try_from_ref(ctx, &self.entity.data_room_dataset_id.as_local_ref()).await? {
            Some(ds) => Ok(ds),
            None => Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Dataset inaccessible".into(),
            ))),
        }
    }

    /// Project's announcements dataset
    #[tracing::instrument(level = "info", name = MoleculeProject_announcements, skip_all)]
    pub async fn announcements(&self, ctx: &Context<'_>) -> Result<Dataset> {
        match Dataset::try_from_ref(ctx, &self.entity.announcements_dataset_id.as_local_ref())
            .await?
        {
            Some(ds) => Ok(ds),
            None => Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Dataset inaccessible".into(),
            ))),
        }
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProject_activity, skip_all)]
    pub async fn activity(
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
        events.append(&mut project.get_activity_announcements(ctx, limit).await?);
        events.append(&mut project.get_activity_data_room(ctx, limit).await?);
        events.append(&mut project.get_activity_files(ctx, limit).await?);

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
        self.entry.entity.system_time
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
        self.entry.entity.system_time
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
        self.new_entry.entity.system_time
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

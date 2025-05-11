// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu::domain;
use kamu_core::DatasetRegistryExt as _;

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
                                "ipnft_address STRING",
                                "ipnft_token_id BIGINT",
                                "ipnft_uid STRING",
                                "ipt_address STRING",
                                "ipt_symbol STRING",
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
        super::Collection::dataset_shapshot(
            alias,
            vec![ColumnInput {
                name: "molecule_change_by".into(),
                data_type: DataTypeInput {
                    ddl: "STRING".into(),
                },
            }],
            vec![],
        )
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

    pub async fn get_projects_dataset(ctx: &Context<'_>) -> Result<domain::ResolvedDataset> {
        let dataset_reg = from_catalog_n!(ctx, dyn domain::DatasetRegistry);

        let projects_dataset = dataset_reg
            .get_dataset_by_ref(&"molecule/projects".parse().unwrap())
            .await
            .int_err()?;

        Ok(projects_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Molecule {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = Molecule_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProject>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // Resolve projects dataset
        let projects_dataset = Self::get_projects_dataset(ctx).await?;

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

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // Resolve projects dataset
        let projects_dataset = Self::get_projects_dataset(ctx).await?;

        // Query data
        let query_res = query_svc
            .get_data(
                &projects_dataset.get_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(MoleculeProjectConnection::new(Vec::new(), 0, per_page, 0));
        };

        let df = odf::utils::data::changelog::project(
            df,
            &["account_id".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let total_count = df.clone().count().await.int_err()?;
        let df = df
            .sort(vec![col("ipt_symbol").sort(true, false)])
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeProject {
    #[graphql(skip)]
    pub account_id: odf::AccountID,

    /// System time when this version was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this version was created/updated
    pub event_time: DateTime<Utc>,

    /// Address of the IPNFT contract
    pub ipnft_address: String,

    /// Token ID withing the IPNFT contract
    pub ipnft_token_id: usize,

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub ipnft_uid: String,

    /// Address of the associated IPT contract
    pub ipt_address: String,

    /// Symbolic name of the IPT token
    pub ipt_symbol: String,

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

        let ipnft_address = record
            .remove("ipnft_address")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipnft_token_id = record.remove("ipnft_token_id").unwrap().as_i64().unwrap();

        let ipnft_uid = record
            .remove("ipnft_uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipt_address = record
            .remove("ipt_address")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let ipt_symbol = record
            .remove("ipt_symbol")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

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
            ipnft_address,
            ipnft_token_id: usize::try_from(ipnft_token_id).unwrap(),
            ipnft_uid,
            ipt_address,
            ipt_symbol,
            data_room_dataset_id,
            announcements_dataset_id,
        }
    }

    pub fn to_record_data(&self) -> serde_json::Value {
        let mut r = serde_json::Value::Object(Default::default());
        r["account_id"] = self.account_id.as_did_str().to_string().into();
        r["ipnft_address"] = self.ipnft_address.clone().into();
        r["ipnft_token_id"] = self.ipnft_token_id.into();
        r["ipnft_uid"] = self.ipnft_uid.clone().into();
        r["ipt_address"] = self.ipt_address.clone().into();
        r["ipt_symbol"] = self.ipt_symbol.clone().into();
        r["data_room_dataset_id"] = self.data_room_dataset_id.as_did_str().to_string().into();
        r["announcements_dataset_id"] = self
            .announcements_dataset_id
            .as_did_str()
            .to_string()
            .into();
        r
    }

    pub fn to_bytes(&self, op: odf::metadata::OperationType) -> bytes::Bytes {
        let mut record = self.to_record_data();
        record["op"] = u8::from(op).into();

        let buf = record.to_string().into_bytes();
        bytes::Bytes::from_owner(buf)
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[ComplexObject]
impl MoleculeProject {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MoleculeProject,
    MoleculeProjectConnection,
    MoleculeProjectEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

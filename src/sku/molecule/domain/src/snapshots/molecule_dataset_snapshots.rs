// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{DatasetColumn, DatasetSnapshots};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetSnapshots {}

impl MoleculeDatasetSnapshots {
    pub fn projects(alias: odf::DatasetAlias) -> odf::metadata::DatasetSnapshot {
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

    pub fn data_room_v2(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        // Extra columns
        const COLUMN_NAME_CHANGE_BY: &str = "molecule_change_by";
        // Denormalized values from the latest file state
        const COLUMN_NAME_ACCESS_LEVEL: &str = "molecule_access_level";
        const COLUMN_NAME_CONTENT_TYPE: &str = "content_type";
        const COLUMN_NAME_CONTENT_HASH: &str = "content_hash";
        const COLUMN_NAME_CONTENT_LENGTH: &str = "content_length";
        const COLUMN_NAME_DESCRIPTION: &str = "description";
        const COLUMN_NAME_CATEGORIES: &str = "categories";
        const COLUMN_NAME_TAGS: &str = "tags";
        const COLUMN_NAME_VERSION: &str = "version";

        DatasetSnapshots::collection(
            alias,
            vec![
                // Extra columns
                DatasetColumn::string(COLUMN_NAME_CHANGE_BY),
                // Denormalized values from the latest file state
                DatasetColumn::string(COLUMN_NAME_ACCESS_LEVEL),
                DatasetColumn::string(COLUMN_NAME_CONTENT_TYPE),
                DatasetColumn::string(COLUMN_NAME_CONTENT_HASH),
                DatasetColumn::int(COLUMN_NAME_CONTENT_LENGTH),
                DatasetColumn::string(COLUMN_NAME_DESCRIPTION),
                DatasetColumn::string_array(COLUMN_NAME_CATEGORIES),
                DatasetColumn::string_array(COLUMN_NAME_TAGS),
                DatasetColumn::int(COLUMN_NAME_VERSION),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn versioned_file_v2(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        // Extra columns
        const COLUMN_NAME_ACCESS_LEVEL: &str = "molecule_access_level";
        const COLUMN_NAME_CHANGE_BY: &str = "molecule_change_by";
        // Extended metadata
        const COLUMN_NAME_DESCRIPTION: &str = "description";
        const COLUMN_NAME_CATEGORIES: &str = "categories";
        const COLUMN_NAME_TAGS: &str = "tags";
        // Semantic search
        const COLUMN_NAME_CONTENT_TEXT: &str = "content_text";
        // E2EE
        const COLUMN_NAME_ENCRYPTION_METADATA: &str = "encryption_metadata";

        DatasetSnapshots::versioned_file(
            alias,
            vec![
                // Extra columns
                DatasetColumn::string(COLUMN_NAME_ACCESS_LEVEL),
                DatasetColumn::string(COLUMN_NAME_CHANGE_BY),
                DatasetColumn::string(COLUMN_NAME_DESCRIPTION),
                // Extended metadata
                DatasetColumn::string_array(COLUMN_NAME_CATEGORIES),
                DatasetColumn::string_array(COLUMN_NAME_TAGS),
                // Semantic search
                DatasetColumn::string(COLUMN_NAME_CONTENT_TEXT),
                // E2EE
                DatasetColumn::string(COLUMN_NAME_ENCRYPTION_METADATA),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn announcements(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

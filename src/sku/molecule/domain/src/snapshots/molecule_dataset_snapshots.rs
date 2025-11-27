// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{CollectionEntity, DatasetColumn, VersionedFileEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetSnapshots;

impl MoleculeDatasetSnapshots {
    pub fn projects_alias(molecule_account_name: odf::AccountName) -> odf::DatasetAlias {
        odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked("projects"),
        )
    }

    pub fn projects(molecule_account_name: odf::AccountName) -> odf::metadata::DatasetSnapshot {
        let alias = Self::projects_alias(molecule_account_name);

        let schema = odf::schema::DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                odf::schema::DataField::string("account_id"),
                odf::schema::DataField::string("ipnft_symbol"),
                odf::schema::DataField::string("ipnft_uid"),
                odf::schema::DataField::string("ipnft_address"),
                odf::schema::DataField::string("ipnft_token_id"),
                odf::schema::DataField::string("data_room_dataset_id"),
                odf::schema::DataField::string("announcements_dataset_id"),
            ])
            .build()
            .expect("Schema is always valid as there are no user inputs");

        odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::MetadataEvent::SetDataSchema(odf::metadata::SetDataSchema::new(schema)),
                odf::metadata::AddPushSource {
                    source_name: "default".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(
                            [
                                "op INT NOT NULL",
                                "account_id STRING NOT NULL",
                                "ipnft_symbol STRING NOT NULL",
                                "ipnft_uid STRING NOT NULL",
                                "ipnft_address STRING NOT NULL",
                                "ipnft_token_id STRING NOT NULL",
                                "data_room_dataset_id STRING NOT NULL",
                                "announcements_dataset_id STRING NOT NULL",
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

    pub fn data_room_v2(molecule_account_name: odf::AccountName) -> odf::DatasetSnapshot {
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

        let alias = odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked("data-room"),
        );

        CollectionEntity::dataset_snapshot(
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

        VersionedFileEntity::dataset_snapshot(
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

    pub fn announcements(molecule_account_name: odf::AccountName) -> odf::DatasetSnapshot {
        let alias = odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked("announcements"),
        );

        odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: "default".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(
                            [
                                "op INT NOT NULL",
                                "announcement_id STRING NOT NULL",
                                "headline STRING NOT NULL",
                                "body STRING NOT NULL",
                                "attachments Array<STRING> NOT NULL",
                                "molecule_access_level STRING NOT NULL",
                                "molecule_change_by STRING NOT NULL",
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

    pub fn global_data_room_activity_alias(
        molecule_account_name: odf::AccountName,
    ) -> odf::DatasetAlias {
        odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked("data-room-activity"),
        )
    }

    pub fn global_data_room_activity(
        molecule_account_name: odf::AccountName,
    ) -> odf::DatasetSnapshot {
        let alias = Self::global_data_room_activity_alias(molecule_account_name);

        let schema = odf::schema::DataSchema::builder()
            // Add these columns but use Append strategy
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                odf::schema::DataField::string("activity_type"),
                odf::schema::DataField::string("ipnft_uid"),
                odf::schema::DataField::string("path"),
                odf::schema::DataField::string("ref"),
                odf::schema::DataField::u32("version"),
                odf::schema::DataField::string("molecule_change_by"),
                odf::schema::DataField::string("molecule_access_level"),
                odf::schema::DataField::string("content_type").optional(),
                odf::schema::DataField::u64("content_length"),
                odf::schema::DataField::string("content_hash"),
                odf::schema::DataField::string("description").optional(),
                // TODO: DataWriterDataFusion::validate_schema_compatible(): detects incompatible
                //       types in the following columns during ingesting:
                //       REQUIRED group categories (LIST) {
                //         REPEATED group list {
                //           OPTIONAL BYTE_ARRAY item (STRING); <-- (1)
                //          }
                //       }
                //       REQUIRED group tags (LIST) {
                //         REPEATED group list {
                //           OPTIONAL BYTE_ARRAY item (STRING); <-- (2)
                //         }
                //       }
                //
                // (1), (2) OPTIONAL by some reason (in new data), but it's not in data frame.
                //          Expected: REQUIRED like it should be.
                //
                //       By the reason of this, we need to add these columns as optional into data
                //       schema.
                // -->
                odf::schema::DataField::list(
                    "categories",
                    odf::schema::DataType::optional(odf::schema::DataType::string()),
                ),
                odf::schema::DataField::list(
                    "tags",
                    odf::schema::DataType::optional(odf::schema::DataType::string()),
                ),
                // <--
            ])
            .build()
            .expect("Schema is always valid as there are no user inputs");

        odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::MetadataEvent::SetDataSchema(odf::metadata::SetDataSchema::new(schema)),
                odf::metadata::AddPushSource {
                    source_name: "default".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(
                            [
                                "activity_type STRING NOT NULL",
                                "ipnft_uid STRING NOT NULL",
                                "path STRING NOT NULL",
                                "ref STRING NOT NULL",
                                "version INT UNSIGNED NOT NULL",
                                "molecule_change_by STRING NOT NULL",
                                "molecule_access_level STRING NOT NULL",
                                "content_type STRING NOT NULL",
                                "content_length BIGINT UNSIGNED NOT NULL",
                                "content_hash STRING",
                                "description STRING",
                                "categories Array<STRING> NOT NULL",
                                "tags Array<STRING> NOT NULL",
                            ]
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        ),
                        ..Default::default()
                    }
                    .into(),
                    // TODO: Set next version based on the previous?
                    //       Remove version from read step in this case
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyAppend {}.into(),
                }
                .into(),
                odf::metadata::SetInfo {
                    description: Some("Data room activity".into()),
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
                                # Data room activity

                                Dataset containing activity across all project data rooms in single feed.
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

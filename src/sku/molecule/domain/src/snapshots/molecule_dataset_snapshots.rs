// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::DatasetSnapshots;
use odf::schema::{DataField, DataSchema, DataType};

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

        let schema = DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                DataField::string("ocl_id"),
                DataField::string("symbol"),
                DataField::string("odf_account_id"),
                DataField::string("odf_data_room_dataset_id"),
                DataField::string("odf_announcements_dataset_id"),
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
                        schema: Some(DataSchema::new(vec![
                            DataField::i32("op"),
                            DataField::string("ocl_id"),
                            DataField::string("symbol"),
                            DataField::string("odf_account_id"),
                            DataField::string("odf_data_room_dataset_id"),
                            DataField::string("odf_announcements_dataset_id"),
                        ])),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyChangelogStream {
                        primary_key: vec!["ocl_id".to_string()],
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

    pub fn data_room(molecule_account_name: odf::AccountName) -> odf::DatasetSnapshot {
        const DATASET_NAME: &str = "data-room";
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
            odf::DatasetName::new_unchecked(DATASET_NAME),
        );

        DatasetSnapshots::collection(
            alias,
            vec![
                // Extra columns
                DataField::string(COLUMN_NAME_CHANGE_BY).optional(),
                // Denormalized values from the latest file state
                DataField::string(COLUMN_NAME_ACCESS_LEVEL).optional(),
                DataField::string(COLUMN_NAME_CONTENT_TYPE).optional(),
                DataField::string(COLUMN_NAME_CONTENT_HASH).optional(),
                DataField::i32(COLUMN_NAME_CONTENT_LENGTH).optional(), // NOTE: i32 instead of u64 for legacy reasons
                DataField::string(COLUMN_NAME_DESCRIPTION).optional(),
                DataField::list(COLUMN_NAME_CATEGORIES, DataType::string().optional()).optional(),
                DataField::list(COLUMN_NAME_TAGS, DataType::string().optional()).optional(),
                DataField::i32(COLUMN_NAME_VERSION).optional(),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn versioned_file(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
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
                DataField::string(COLUMN_NAME_ACCESS_LEVEL).optional(),
                DataField::string(COLUMN_NAME_CHANGE_BY).optional(),
                DataField::string(COLUMN_NAME_DESCRIPTION).optional(),
                // Extended metadata
                DataField::list(COLUMN_NAME_CATEGORIES, DataType::string().optional()).optional(),
                DataField::list(COLUMN_NAME_TAGS, DataType::string().optional()).optional(),
                // Semantic search
                DataField::string(COLUMN_NAME_CONTENT_TEXT).optional(),
                // E2EE
                DataField::string(COLUMN_NAME_ENCRYPTION_METADATA).optional(),
            ],
            Vec::new(),
        )
        .expect("Schema is always valid as there are no user inputs")
    }

    pub fn announcements(project_account_name: odf::AccountName) -> odf::DatasetSnapshot {
        const DATASET_NAME: &str = "announcements";

        let alias = odf::DatasetAlias::new(
            Some(project_account_name),
            odf::DatasetName::new_unchecked(DATASET_NAME),
        );

        let schema = DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                DataField::string("announcement_id"),
                DataField::string("headline"),
                DataField::string("body"),
                // TODO: Link as odf::DatasetIDs not just strings
                DataField::list("attachments", DataType::string()),
                DataField::string("molecule_access_level"),
                DataField::string("molecule_change_by"),
                // NOTE: These were added in V2 and must be optional for schema compatibility
                DataField::list("categories", DataType::string()).optional(),
                DataField::list("tags", DataType::string()).optional(),
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
                        schema: Some(DataSchema::new(vec![
                            DataField::i32("op"),
                            DataField::string("announcement_id"),
                            DataField::string("headline"),
                            DataField::string("body"),
                            DataField::list("attachments", DataType::string().optional()),
                            DataField::string("molecule_access_level"),
                            DataField::string("molecule_change_by"),
                            DataField::list("categories", DataType::string().optional()),
                            DataField::list("tags", DataType::string().optional()),
                        ])),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    // TODO: append strategy?
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

    pub fn global_announcements_alias(
        molecule_account_name: odf::AccountName,
    ) -> odf::DatasetAlias {
        const DATASET_NAME: &str = "announcements";

        odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked(DATASET_NAME),
        )
    }

    pub fn global_announcements(molecule_account_name: odf::AccountName) -> odf::DatasetSnapshot {
        const DATASET_NAME: &str = "announcements";

        let alias = odf::DatasetAlias::new(
            Some(molecule_account_name),
            odf::DatasetName::new_unchecked(DATASET_NAME),
        );

        let schema = DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                DataField::string("ocl_id"),
                DataField::string("announcement_id"),
                DataField::string("headline"),
                DataField::string("body"),
                // TODO: Link as odf::DatasetIDs not just strings
                DataField::list("attachments", DataType::string()),
                DataField::string("molecule_access_level"),
                DataField::string("molecule_change_by"),
                DataField::list("categories", DataType::string()),
                DataField::list("tags", DataType::string()),
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
                        schema: Some(DataSchema::new(vec![
                            DataField::i32("op"),
                            DataField::timestamp_millis_utc("event_time").optional(),
                            DataField::string("ocl_id"),
                            DataField::string("announcement_id"),
                            DataField::string("headline"),
                            DataField::string("body"),
                            DataField::list("attachments", DataType::string().optional()),
                            DataField::string("molecule_access_level"),
                            DataField::string("molecule_change_by"),
                            DataField::list("categories", DataType::string().optional()),
                            DataField::list("tags", DataType::string().optional()),
                        ])),
                        ..Default::default()
                    }
                        .into(),
                    preprocess: None,
                    // TODO: append strategy? if so, remove "op INT NOT NULL" from the schema
                    merge: odf::metadata::MergeStrategyChangelogStream {
                        primary_key: vec!["announcement_id".to_string()],
                    }
                        .into(),
                }
                    .into(),
                odf::metadata::SetInfo {
                    description: Some("Projects announcements".into()),
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
                                # Announcements

                                Dataset automatically combines announcements from individual projects.
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

        let schema = DataSchema::builder()
            // Add these columns but use Append strategy
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                DataField::string("activity_type"),
                DataField::string("ocl_id"),
                DataField::string("path"),
                DataField::string("ref"),
                DataField::u32("version"),
                DataField::string("molecule_change_by"),
                DataField::string("molecule_access_level"),
                DataField::string("content_type").optional(),
                DataField::u64("content_length"),
                DataField::string("content_hash"),
                DataField::string("description").optional(),
                DataField::list("categories", DataType::string()),
                DataField::list("tags", DataType::string()),
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
                        schema: Some(DataSchema::new(vec![
                                DataField::timestamp_millis_utc("event_time").optional(),
                                DataField::string("activity_type"),
                                DataField::string("ocl_id"),
                                DataField::string("path"),
                                DataField::string("ref"),
                                DataField::u32("version"),
                                DataField::string("molecule_change_by"),
                                DataField::string("molecule_access_level"),
                                DataField::string("content_type").optional(),
                                DataField::u64("content_length"),
                                DataField::string("content_hash"),
                                DataField::string("description").optional(),
                                DataField::list("categories", DataType::string().optional()),
                                DataField::list("tags", DataType::string().optional()),
                        ])),
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

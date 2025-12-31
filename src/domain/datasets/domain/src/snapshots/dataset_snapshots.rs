// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::DatasetColumn;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetSnapshots {}

impl DatasetSnapshots {
    pub fn collection(
        alias: odf::DatasetAlias,
        extra_columns: Vec<DatasetColumn>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::InvalidSchema> {
        let extra_columns_ddl: Vec<String> = extra_columns
            .into_iter()
            .map(|c| format!("{} {}", c.name, c.data_type_ddl))
            .collect();

        let extra_columns_schema =
            odf::utils::schema::parse::parse_ddl_to_odf_schema(&extra_columns_ddl.join(", "))?;

        let schema = odf::schema::DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                odf::schema::DataField::string("path").description(
                    "HTTP-like path to a collection entry. Paths start with `/` and can be \
                     nested, with individual path segments url-encoded (e.g. `/foo/bar%20baz`)",
                ),
                odf::schema::DataField::string("ref")
                    .type_ext(odf::schema::ext::DataTypeExt::did())
                    .description("DID that references another dataset"),
            ])
            .extend(extra_columns_schema.fields)
            .extra(odf::schema::ext::DatasetArchetype::Collection)
            .build()?;

        let push_source = odf::metadata::AddPushSource {
            source_name: "default".into(),
            read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                schema: Some(
                    [
                        "op INT",
                        "event_time TIMESTAMP",
                        "path STRING",
                        "ref STRING",
                    ]
                    .into_iter()
                    .map(str::to_string)
                    .chain(extra_columns_ddl)
                    .collect(),
                ),
                ..Default::default()
            }),
            preprocess: None,
            merge: odf::metadata::MergeStrategy::ChangelogStream(
                odf::metadata::MergeStrategyChangelogStream {
                    primary_key: vec!["path".to_string()],
                },
            ),
        };

        Ok(odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: [
                odf::MetadataEvent::SetDataSchema(odf::metadata::SetDataSchema::new(schema)),
                odf::MetadataEvent::AddPushSource(push_source),
            ]
            .into_iter()
            .chain(extra_events)
            .collect(),
        })
    }

    pub fn versioned_file(
        alias: odf::DatasetAlias,
        extra_columns: Vec<DatasetColumn>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::InvalidSchema> {
        let extra_columns_ddl: Vec<String> = extra_columns
            .into_iter()
            .map(|c| format!("{} {}", c.name, c.data_type_ddl))
            .collect();

        let extra_columns_schema =
            odf::utils::schema::parse::parse_ddl_to_odf_schema(&extra_columns_ddl.join(", "))?;

        let schema = odf::schema::DataSchema::builder()
            .with_changelog_system_fields(odf::metadata::DatasetVocabulary::default(), None)
            .extend([
                odf::schema::DataField::i32("version").description(
                    "Sequential identifier assigned to each entry as new versions are uploaded",
                ),
                odf::schema::DataField::string("content_hash")
                    .type_ext(odf::schema::ext::DataTypeExt::object_link(
                        odf::schema::ext::DataTypeExt::multihash(),
                    ))
                    .description("Hash that references the externally-stored object content"),
                odf::schema::DataField::i64("content_length")
                    .description("Size of the linked object in bytes"),
                odf::schema::DataField::string("content_type")
                    .optional()
                    .description("Media type associated with the linked object"),
            ])
            .extend(extra_columns_schema.fields)
            .extra(odf::schema::ext::DatasetArchetype::VersionedFile)
            .build()?;

        let push_source = odf::metadata::AddPushSource {
            source_name: "default".into(),
            read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                schema: Some(
                    [
                        "event_time TIMESTAMP",
                        "version INT",
                        "content_hash STRING",
                        "content_length BIGINT",
                        "content_type STRING",
                    ]
                    .into_iter()
                    .map(str::to_string)
                    .chain(extra_columns_ddl)
                    .collect(),
                ),
                ..Default::default()
            }),
            preprocess: None,
            merge: odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
        };

        Ok(odf::DatasetSnapshot {
            name: alias,
            kind: odf::DatasetKind::Root,
            metadata: [
                odf::MetadataEvent::SetDataSchema(odf::metadata::SetDataSchema::new(schema)),
                odf::MetadataEvent::AddPushSource(push_source),
            ]
            .into_iter()
            .chain(extra_events)
            .collect(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

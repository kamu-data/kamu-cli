// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Extension, Query};
use axum::response::Json;
use comma_separated::CommaSeparatedSet;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use internal_error::*;
use kamu_core::*;

use super::query_types;
use crate::DatasetAliasInPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::Display, strum::EnumString)]
#[strum(serialize_all = "PascalCase")]
#[strum(ascii_case_insensitive)]
pub enum Include {
    /// Last `SetAttachments` event
    Attachments,

    /// Last `SetInfo` event
    Info,

    /// Last `SetLicense` event
    License,

    /// Existing block references
    Refs,

    /// Last `SetDataSchema`
    Schema,

    /// The `Seed` event containing dataset identity
    Seed,

    /// Last `SetVocab` event
    Vocab,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[serde_with::serde_as]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct DatasetMetadataParams {
    /// What information to include in response
    #[param(value_type = Option<String>)]
    #[serde(default = "DatasetMetadataParams::default_include")]
    pub include: CommaSeparatedSet<Include>,

    /// Format to return the schema in
    #[serde(default)]
    pub schema_format: query_types::SchemaFormat,
}

impl DatasetMetadataParams {
    pub fn default_include() -> CommaSeparatedSet<Include> {
        CommaSeparatedSet::from([Include::Seed])
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetMetadataResponse {
    pub output: Output,
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Output {
    #[serde_as(as = "Option<odf::serde::yaml::SetAttachmentsDef>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub attachments: Option<odf::metadata::SetAttachments>,

    #[serde_as(as = "Option<odf::serde::yaml::SetInfoDef>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub info: Option<odf::metadata::SetInfo>,

    #[serde_as(as = "Option<odf::serde::yaml::SetLicenseDef>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub license: Option<odf::metadata::SetLicense>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<String>>)]
    pub refs: Option<Vec<Ref>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = query_types::Schema)]
    pub schema: Option<query_types::Schema>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = query_types::SchemaFormat)]
    pub schema_format: Option<query_types::SchemaFormat>,

    #[serde_as(as = "Option<odf::serde::yaml::SeedDef>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub seed: Option<odf::metadata::Seed>,

    #[serde_as(as = "Option<odf::serde::yaml::DatasetVocabularyDef>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub vocab: Option<odf::metadata::DatasetVocabulary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ref {
    pub name: String,
    pub block_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Access dataset metadata chain
#[utoipa::path(
    get,
    path = "/metadata",
    params(DatasetMetadataParams, DatasetAliasInPath),
    responses((status = OK, body = DatasetMetadataResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn dataset_metadata_handler(
    Extension(catalog): Extension<Catalog>,
    Extension(dataset_ref): Extension<odf::DatasetRef>,
    Query(params): Query<DatasetMetadataParams>,
) -> Result<Json<DatasetMetadataResponse>, ApiError> {
    let dataset_registry = catalog.get_one::<dyn DatasetRegistry>().unwrap();
    let resolved_dataset = dataset_registry
        .get_dataset_by_ref(&dataset_ref)
        .await
        .api_err()?;

    let mut attachments_visitor = params
        .include
        .contains(&Include::Attachments)
        .then(odf::dataset::SearchSetAttachmentsVisitor::new);
    let mut info_visitor = params
        .include
        .contains(&Include::Info)
        .then(odf::dataset::SearchSetInfoVisitor::new);
    let mut license_visitor = params
        .include
        .contains(&Include::License)
        .then(odf::dataset::SearchSetLicenseVisitor::new);
    let mut schema_visitor = params
        .include
        .contains(&Include::Schema)
        .then(odf::dataset::SearchSetDataSchemaVisitor::new);
    let mut seed_visitor = params
        .include
        .contains(&Include::Seed)
        .then(odf::dataset::SearchSeedVisitor::new);
    let mut vocab_visitor = params
        .include
        .contains(&Include::Vocab)
        .then(odf::dataset::SearchSetVocabVisitor::new);

    let mut visitors: [&mut dyn odf::dataset::MetadataChainVisitor<
        Error = odf::dataset::Infallible,
    >; 6] = [
        &mut attachments_visitor,
        &mut info_visitor,
        &mut license_visitor,
        &mut schema_visitor,
        &mut seed_visitor,
        &mut vocab_visitor,
    ];

    use odf::dataset::MetadataChainExt as _;
    resolved_dataset
        .as_metadata_chain()
        .accept(&mut visitors)
        .await
        .int_err()?;

    let attachments =
        attachments_visitor.and_then(odf::dataset::SearchSingleTypedBlockVisitor::into_event);

    let info = info_visitor.and_then(odf::dataset::SearchSingleTypedBlockVisitor::into_event);

    let license = license_visitor.and_then(odf::dataset::SearchSingleTypedBlockVisitor::into_event);

    let (schema, schema_format) = schema_visitor
        .and_then(odf::dataset::SearchSingleTypedBlockVisitor::into_event)
        .map(|schema| schema.schema_as_arrow())
        .transpose()
        .int_err()?
        .map(|schema| {
            (
                query_types::Schema::new(schema, params.schema_format),
                params.schema_format,
            )
        })
        .unzip();

    let seed = seed_visitor.and_then(odf::dataset::SearchSingleTypedBlockVisitor::into_event);

    let vocab = vocab_visitor
        .map(|v| v.into_event().unwrap_or_default())
        .map(Into::into);

    let refs = if !params.include.contains(&Include::Refs) {
        None
    } else {
        resolved_dataset
            .as_metadata_chain()
            .try_get_ref(&odf::BlockRef::Head)
            .await
            .int_err()?
            .map(|head| {
                vec![Ref {
                    name: "head".to_string(),
                    block_hash: head,
                }]
            })
    };

    Ok(Json(DatasetMetadataResponse {
        output: Output {
            attachments,
            info,
            license,
            refs,
            schema,
            schema_format,
            seed,
            vocab,
        },
    }))
}

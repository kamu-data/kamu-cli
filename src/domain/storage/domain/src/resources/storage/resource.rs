// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use kamu_resources::{
    DeclarativeResource,
    DeclarativeResourceState,
    ResourceListColumnDataType,
    ResourceListColumnDefinition,
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourceListColumnVisibility,
    ResourcePresentation,
    ResourcePresentationDefinition,
    ResourceSchemaProvider,
    ResourceSelectorName,
    TypeUri,
};

use crate::{StorageEventStore, StorageProviderSpec, StorageSpec, StorageState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(pub(crate) Aggregate<StorageState, dyn StorageEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    /// Canonical schema URL as a `&'static str`.
    ///
    /// Used as the const dill-registry key (dill `#[meta]` requires a const);
    /// the typed identity is [`Self::schema`] returning a `TypeUri` derived
    /// from this same const. There is no ODF-generated `Storage` type yet, so
    /// the literal lives here.
    pub const SCHEMA_STR: &'static str =
        "https://opendatafabric.org/schemas/storage/v1alpha1/Storage";

    pub const CANONICAL_SELECTOR_NAME_STR: &'static str = "storages";
    pub const CANONICAL_SELECTOR_NAME: ResourceSelectorName =
        ResourceSelectorName::new_unchecked_static(Self::CANONICAL_SELECTOR_NAME_STR);

    pub const SELECTOR_ALIAS_STRS: &'static [&'static str] = &["st", "storage"];
    pub const SELECTOR_ALIASES: &'static [ResourceSelectorName] = &[
        ResourceSelectorName::new_unchecked_static("st"),
        ResourceSelectorName::new_unchecked_static("storage"),
    ];

    fn provider_detail(spec: &StorageSpec) -> String {
        match &spec.provider {
            StorageProviderSpec::LocalFs(local_fs) => local_fs.workspace_path.clone(),
            StorageProviderSpec::S3(s3) => s3.bucket.to_string(),
            StorageProviderSpec::Ipfs(ipfs) => ipfs
                .gateway
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_string()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSchemaProvider for StorageResource {
    fn schema() -> &'static TypeUri {
        static STORAGE_SCHEMA: std::sync::LazyLock<TypeUri> =
            std::sync::LazyLock::new(|| TypeUri::new_unchecked(StorageResource::SCHEMA_STR));
        &STORAGE_SCHEMA
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Spec = StorageSpec;
    type ResourceState = StorageState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourcePresentation for StorageResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::CANONICAL_SELECTOR_NAME,
        Self::SELECTOR_ALIASES,
        &[
            ResourceListColumnDefinition {
                key: "provider",
                header: "Provider",
                data_type: ResourceListColumnDataType::String,
                visibility: ResourceListColumnVisibility::Default,
            },
            ResourceListColumnDefinition {
                key: "detail",
                header: "Detail",
                data_type: ResourceListColumnDataType::String,
                visibility: ResourceListColumnVisibility::WideOnly,
            },
        ],
    );

    fn list_column_values(state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        vec![
            ResourceListColumnValueView {
                key: "provider".to_string(),
                value: ResourceListColumnValue::String(state.spec().provider.kind().to_string()),
            },
            ResourceListColumnValueView {
                key: "detail".to_string(),
                value: ResourceListColumnValue::String(Self::provider_detail(state.spec())),
            },
        ]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn selector_constants_stay_in_sync() {
        assert_eq!(
            StorageResource::CANONICAL_SELECTOR_NAME_STR,
            StorageResource::CANONICAL_SELECTOR_NAME.as_str()
        );
        assert_eq!(
            StorageResource::SELECTOR_ALIAS_STRS,
            StorageResource::SELECTOR_ALIASES
                .iter()
                .map(ResourceSelectorName::as_str)
                .collect::<Vec<_>>()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

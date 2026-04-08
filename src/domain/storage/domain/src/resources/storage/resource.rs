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
    ResourceApiVersion,
    ResourceListColumnDataType,
    ResourceListColumnDefinition,
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourceListColumnVisibility,
    ResourcePresentation,
    ResourcePresentationDefinition,
    ResourceType,
};

use crate::{StorageEventStore, StorageProviderSpec, StorageSpec, StorageState, StorageStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(pub(crate) Aggregate<StorageState, dyn StorageEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    pub const RESOURCE_TYPE: &'static str = "Storage";
    pub const RESOURCE_NAME: &'static str = "storages";
    pub const RESOURCE_SHORT_NAMES: &'static [&'static str] = &["st", "storage"];
    pub const API_VERSION: &'static str = "kamu.dev/v1alpha1";

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

impl ResourceType for StorageResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

impl ResourceApiVersion for StorageResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type ResourceState = StorageState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourcePresentation for StorageResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::RESOURCE_NAME,
        Self::RESOURCE_SHORT_NAMES,
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
                value: ResourceListColumnValue::String(state.status().provider_kind.to_string()),
            },
            ResourceListColumnValueView {
                key: "detail".to_string(),
                value: ResourceListColumnValue::String(Self::provider_detail(state.spec())),
            },
        ]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    TypeUri,
};

use crate::{SecretSetEventStore, SecretSetSpec, SecretSetSpecInput, SecretSetState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(pub(crate) Aggregate<SecretSetState, dyn SecretSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    /// Canonical schema URL as a `&'static str`, sourced from the ODF codegen.
    ///
    /// Used as the const dill-registry key (dill `#[meta]` requires a const);
    /// the typed identity is [`Self::schema`] returning a `TypeUri`.
    pub const SCHEMA_STR: &'static str = odf::metadata::config::SecretSet::schema_str();

    // "secretset" (lowercase-of-canonical) is deliberately omitted: matching is
    // already case-insensitive, so it would collide with the canonical name.
    kamu_resources::declare_resource_selector_constants!("SecretSet", ["secretsets", "ss"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSchemaProvider for SecretSetResource {
    fn schema() -> &'static TypeUri {
        odf::metadata::config::SecretSet::schema()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type SpecInput = SecretSetSpecInput;
    type ResourceState = SecretSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourcePresentation for SecretSetResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::CANONICAL_SELECTOR_NAME,
        Self::SELECTOR_ALIASES,
        &[ResourceListColumnDefinition {
            key: "secrets",
            header: "Secrets",
            data_type: ResourceListColumnDataType::UInt64,
            visibility: ResourceListColumnVisibility::Default,
        }],
    );

    fn list_column_values(state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        vec![ResourceListColumnValueView {
            key: "secrets".to_string(),
            value: ResourceListColumnValue::UInt64(
                u64::try_from(state.spec().secrets.entries.len()).unwrap(),
            ),
        }]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

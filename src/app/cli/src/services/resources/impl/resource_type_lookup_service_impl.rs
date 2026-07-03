// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use kamu_resources::ResourceTypeDescriptor;

use crate::CLIError;
use crate::resources::{
    ResourceFacadeFactory,
    ResourceTypeLookupErrorOptions,
    ResourceTypeLookupService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceTypeLookupServiceImpl {
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    cache: Mutex<HashMap<Option<String>, Vec<ResourceTypeDescriptor>>>,
}

#[dill::component(pub)]
#[dill::interface(dyn ResourceTypeLookupService)]
impl ResourceTypeLookupServiceImpl {
    pub fn new(resource_facade_factory: Arc<dyn ResourceFacadeFactory>) -> Self {
        Self {
            resource_facade_factory,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceTypeLookupService for ResourceTypeLookupServiceImpl {
    async fn list_supported_resource_types(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<Vec<ResourceTypeDescriptor>, CLIError> {
        let cache_key = explicit_context_name.map(str::to_owned);

        {
            let cache = self.cache.lock().unwrap();
            if let Some(kinds) = cache.get(&cache_key) {
                return Ok(kinds.clone());
            }
        }

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(explicit_context_name)?;

        let supported_resource_types = resource_facade.list_supported_resource_types().await?;
        Self::assert_unique_selectors(&supported_resource_types);

        self.cache
            .lock()
            .unwrap()
            .insert(cache_key, supported_resource_types.clone());

        Ok(supported_resource_types)
    }

    async fn resolve_type_descriptor(
        &self,
        explicit_context_name: Option<&str>,
        target: &str,
        error_options: ResourceTypeLookupErrorOptions,
    ) -> Result<ResourceTypeDescriptor, CLIError> {
        let supported_resource_types = self
            .list_supported_resource_types(explicit_context_name)
            .await?;

        supported_resource_types
            .iter()
            .find(|descriptor| descriptor.matches_selector(target))
            .cloned()
            .ok_or_else(|| {
                CLIError::usage_error(format!(
                    "{} '{target}'. Supported targets: {}",
                    error_options.unsupported_prefix,
                    Self::supported_targets(
                        &supported_resource_types,
                        &error_options.additional_targets
                    )
                    .join(", ")
                ))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceTypeLookupServiceImpl {
    fn assert_unique_selectors(supported_resource_types: &[ResourceTypeDescriptor]) {
        let mut selector_to_resource_types: HashMap<String, Vec<&str>> = HashMap::new();

        for descriptor in supported_resource_types {
            selector_to_resource_types
                .entry(descriptor.canonical_selector.as_str().to_ascii_lowercase())
                .or_default()
                .push(descriptor.canonical_selector.as_str());

            for alias in &descriptor.selector_aliases {
                selector_to_resource_types
                    .entry(alias.as_str().to_ascii_lowercase())
                    .or_default()
                    .push(descriptor.canonical_selector.as_str());
            }
        }

        let duplicates = selector_to_resource_types
            .into_iter()
            .filter(|(_, kinds)| kinds.len() > 1)
            .collect::<Vec<_>>();

        if duplicates.is_empty() {
            return;
        }

        let mut details = String::new();
        for (idx, (selector, kinds)) in duplicates.into_iter().enumerate() {
            if idx > 0 {
                details.push_str("; ");
            }
            let _ = write!(&mut details, "{selector} => {}", kinds.join(", "));
        }

        unreachable!("Duplicate resource kind selectors returned by backend: {details}");
    }

    fn supported_targets(
        supported_resource_types: &[ResourceTypeDescriptor],
        additional_targets: &[String],
    ) -> Vec<String> {
        let mut targets = additional_targets.to_vec();

        for descriptor in supported_resource_types {
            targets.push(descriptor.canonical_selector.to_string());
            targets.extend(descriptor.selector_aliases.iter().map(ToString::to_string));
        }

        targets.sort();
        targets.dedup();
        targets
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

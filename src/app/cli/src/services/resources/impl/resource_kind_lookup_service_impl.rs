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

use kamu_resources::ResourceKindDescriptor;

use crate::CLIError;
use crate::resources::{
    ResourceFacadeFactory,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceKindLookupServiceImpl {
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    cache: Mutex<HashMap<Option<String>, Vec<ResourceKindDescriptor>>>,
}

#[dill::component(pub)]
#[dill::interface(dyn ResourceKindLookupService)]
impl ResourceKindLookupServiceImpl {
    pub fn new(resource_facade_factory: Arc<dyn ResourceFacadeFactory>) -> Self {
        Self {
            resource_facade_factory,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceKindLookupService for ResourceKindLookupServiceImpl {
    async fn list_supported_kinds(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<Vec<ResourceKindDescriptor>, CLIError> {
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

        let supported_kinds = resource_facade.list_supported_kinds().await?;
        Self::assert_unique_selectors(&supported_kinds);

        self.cache
            .lock()
            .unwrap()
            .insert(cache_key, supported_kinds.clone());

        Ok(supported_kinds)
    }

    async fn resolve_kind_descriptor(
        &self,
        explicit_context_name: Option<&str>,
        target: &str,
        error_options: ResourceKindLookupErrorOptions,
    ) -> Result<ResourceKindDescriptor, CLIError> {
        let supported_kinds = self.list_supported_kinds(explicit_context_name).await?;

        supported_kinds
            .iter()
            .find(|descriptor| descriptor.matches_selector(target))
            .cloned()
            .ok_or_else(|| {
                CLIError::usage_error(format!(
                    "{} '{target}'. Supported targets: {}",
                    error_options.unsupported_prefix,
                    Self::supported_targets(&supported_kinds, &error_options.additional_targets)
                        .join(", ")
                ))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceKindLookupServiceImpl {
    fn assert_unique_selectors(supported_kinds: &[ResourceKindDescriptor]) {
        let mut selector_to_kinds: HashMap<String, Vec<&str>> = HashMap::new();

        for descriptor in supported_kinds {
            selector_to_kinds
                .entry(descriptor.name.to_ascii_lowercase())
                .or_default()
                .push(descriptor.name.as_str());

            for short_name in &descriptor.short_names {
                selector_to_kinds
                    .entry(short_name.to_ascii_lowercase())
                    .or_default()
                    .push(descriptor.name.as_str());
            }
        }

        let duplicates = selector_to_kinds
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
        supported_kinds: &[ResourceKindDescriptor],
        additional_targets: &[String],
    ) -> Vec<String> {
        let mut targets = additional_targets.to_vec();

        for descriptor in supported_kinds {
            targets.push(descriptor.name.clone());
            targets.extend(descriptor.short_names.iter().cloned());
        }

        targets.sort();
        targets.dedup();
        targets
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

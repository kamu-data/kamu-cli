// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use kamu::domain::*;
use kamu_datasets::DatasetRegistry;
use kamu_resources::ResourceQuery;

use super::{CLIError, Command, ListDatasetsCommand, ListResourcesCommand, ListResourcesScope};
use crate::accounts;
use crate::output::OutputConfig;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    ANY_SELECTOR,
    BareTypePolicy,
    DATASETS_TARGET,
    ResourceFacadeFactory,
    ResourceLabelSelectorParser,
    ResourceSelectionScanner,
    ResourceTypeLookupErrorOptions,
    ResourceTypeLookupService,
    is_dataset_target,
    is_resource_id,
    usage_error_at,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ListCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_statistics_service: Arc<dyn kamu_datasets::DatasetStatisticsService>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_type_lookup_service: Arc<dyn ResourceTypeLookupService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,

    #[dill::component(explicit)]
    current_account: accounts::CurrentAccountIndication,

    #[dill::component(explicit)]
    related_account: accounts::RelatedAccountIndication,

    #[dill::component(explicit)]
    targets: Vec<String>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    detail_level: u8,

    #[dill::component(explicit)]
    max_results: Option<NonZeroUsize>,

    #[dill::component(explicit)]
    unbounded: bool,

    #[dill::component(explicit)]
    label_selectors: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ListCommand {
    fn mode(&self) -> Result<ListMode, CLIError> {
        Self::resolve_mode(&self.targets)
    }

    fn resolve_mode(targets: &[String]) -> Result<ListMode, CLIError> {
        if targets.is_empty() {
            return Ok(ListMode::Datasets);
        }

        if targets
            .iter()
            .any(|target| is_dataset_target(Self::type_half(target)))
        {
            if targets.len() > 1 {
                return Err(CLIError::usage_error(
                    "Cannot mix `datasets` with resource selectors in the same command",
                ));
            }
            if targets[0].contains('/') {
                return Err(CLIError::usage_error(
                    "Datasets do not support `type/name` selectors",
                ));
            }
            return Ok(ListMode::Datasets);
        }

        // A bare `UUIDv4` names one resource across every type, mirroring
        // `kamu get <uuid>`. Checked before type lookup, which would otherwise
        // reject it as an unknown target.
        if targets.len() == 1
            && !targets[0].contains('/')
            && let Some(query) = Self::id_query(&targets[0])
        {
            return Ok(ListMode::ResourcesAll(Some(query)));
        }

        let parsed = targets
            .iter()
            .map(|target| Self::split_target(target))
            .collect::<Result<Vec<_>, _>>()?;

        if parsed
            .iter()
            .any(|(type_half, _)| *type_half == ANY_SELECTOR)
        {
            if parsed.len() > 1 {
                return Err(CLIError::usage_error(format!(
                    "`{ANY_SELECTOR}` already spans every resource type, so it cannot be combined \
                     with other selectors"
                )));
            }
            return Ok(ListMode::ResourcesAll(parsed[0].1.clone()));
        }

        Ok(ListMode::ResourcesByTypes(
            parsed
                .into_iter()
                .map(|(type_half, query)| (type_half.to_owned(), query))
                .collect(),
        ))
    }

    /// Picks the scope from *request* arity, never from the results: one named
    /// type keeps that type's own columns, more than one falls back to generic
    /// columns plus `Type`. Letting the results decide would make the output
    /// schema depend on the data.
    fn scope_for_resolved_types(
        mut resolved: Vec<(
            kamu_resources::ResourceTypeDescriptor,
            Option<ResourceQuery>,
        )>,
    ) -> ListResourcesScope {
        if resolved.len() == 1 {
            let (type_descriptor, query) = resolved.pop().expect("checked to be non-empty");
            ListResourcesScope::ByType(type_descriptor, query)
        } else {
            ListResourcesScope::Types(resolved)
        }
    }

    /// The type half of a `type[/name]` target.
    ///
    /// Used before validation, to spot the `datasets` target, so a malformed
    /// target falls back to itself and is rejected later by
    /// [`Self::split_target`].
    fn type_half(target: &str) -> &str {
        ResourceSelectionScanner::scan_selector_arg(target, BareTypePolicy::Allow)
            .map_or(target, |selector| selector.type_half)
    }

    /// Splits `type[/name]`, classifying the name half as an ID or a pattern.
    ///
    /// Unlike `get`/`delete`, a bare `type` is legal here and means "enumerate
    /// this type".
    fn split_target(target: &str) -> Result<(&str, Option<ResourceQuery>), CLIError> {
        let selector = ResourceSelectionScanner::scan_selector_arg(target, BareTypePolicy::Allow)
            .map_err(|err| {
            usage_error_at("resource selector", target, err.offset, &err.message)
        })?;

        Ok((selector.type_half, selector.name_half.map(Self::name_query)))
    }

    /// An ID query when the input spells a `UUIDv4`, otherwise `None`.
    fn id_query(input: &str) -> Option<ResourceQuery> {
        is_resource_id(input).then(|| {
            ResourceQuery::ExactIds(vec![kamu_resources::ResourceID::new(
                uuid::Uuid::parse_str(input).expect("checked to be a UUID"),
            )])
        })
    }

    /// An ID is matched exactly; anything else is a `%` name pattern, so an
    /// exact name is just a pattern without wildcards.
    fn name_query(name_half: &str) -> ResourceQuery {
        Self::id_query(name_half)
            .unwrap_or_else(|| ResourceQuery::NamePattern(name_half.to_owned()))
    }

    fn make_list_datasets_command(&self) -> ListDatasetsCommand {
        ListDatasetsCommand::new(
            self.tenancy_config,
            self.dataset_registry.clone(),
            self.dataset_statistics_service.clone(),
            self.remote_alias_reg.clone(),
            self.rebac_service.clone(),
            self.current_account.clone(),
            self.related_account.clone(),
            self.output_config.clone(),
            self.detail_level,
            if self.unbounded {
                None
            } else {
                self.max_results
            },
        )
    }

    async fn resolve_list_resources_command(
        &self,
        mode: ListMode,
    ) -> Result<ListResourcesCommand, CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let scope = match mode {
            ListMode::ResourcesAll(query) => ListResourcesScope::All(query),
            ListMode::ResourcesByTypes(type_queries) => {
                let mut resolved = Vec::with_capacity(type_queries.len());
                for (type_half, query) in type_queries {
                    resolved.push((self.resolve_type_descriptor(&type_half).await?, query));
                }

                Self::scope_for_resolved_types(resolved)
            }
            ListMode::Datasets => unreachable!(),
        };

        Ok(ListResourcesCommand::new(
            resource_facade,
            self.resource_context_reporter.clone(),
            resolved_context,
            self.related_account.clone(),
            scope,
            self.output_config.clone(),
            self.detail_level,
            if self.unbounded {
                None
            } else {
                self.max_results
            },
            ResourceLabelSelectorParser::parse(&self.label_selectors)?,
        ))
    }

    async fn resolve_type_descriptor(
        &self,
        type_half: &str,
    ) -> Result<kamu_resources::ResourceTypeDescriptor, CLIError> {
        self.resource_type_lookup_service
            .resolve_type_descriptor(
                self.explicit_context_name.as_deref(),
                type_half,
                ResourceTypeLookupErrorOptions::new("Unsupported list target")
                    .with_additional_targets([DATASETS_TARGET, ANY_SELECTOR]),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ListCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        match self.mode()? {
            ListMode::Datasets => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only when listing resources",
                    ));
                }

                if !self.label_selectors.is_empty() {
                    return Err(CLIError::usage_error(
                        "Label selectors are not supported when listing datasets",
                    ));
                }

                self.make_list_datasets_command().validate_args().await
            }
            ListMode::ResourcesAll(_) | ListMode::ResourcesByTypes(_) => {
                if self.related_account.is_explicit() {
                    return Err(CLIError::usage_error(
                        "Listing resources does not support --target-account or --all-accounts",
                    ));
                }

                Ok(())
            }
        }
    }

    async fn run(&self) -> Result<(), CLIError> {
        let mode = self.mode()?;
        match mode {
            ListMode::Datasets => self.make_list_datasets_command().run().await,
            ListMode::ResourcesAll(_) | ListMode::ResourcesByTypes(_) => {
                self.resolve_list_resources_command(mode).await?.run().await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListMode {
    Datasets,
    /// Every type, optionally narrowed by one query.
    ResourcesAll(Option<ResourceQuery>),
    /// One or more named types, each with its own query.
    ResourcesByTypes(Vec<(String, Option<ResourceQuery>)>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    fn targets(values: &[&str]) -> Vec<String> {
        values.iter().map(ToString::to_string).collect()
    }

    fn resolve(values: &[&str]) -> Result<ListMode, CLIError> {
        ListCommand::resolve_mode(&targets(values))
    }

    fn uuid_v4() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn descriptor(canonical: &str) -> kamu_resources::ResourceTypeDescriptor {
        kamu_resources::ResourceTypeDescriptor {
            canonical_selector: kamu_resources::ResourceSelectorName::new(canonical).unwrap(),
            selector_aliases: Vec::new(),
            schema: kamu_resources::TypeUri::new_unchecked(format!("dev.kamu/{canonical}/v1")),
            list_columns: Vec::new(),
        }
    }

    fn resolved(
        canonicals: &[&str],
    ) -> Vec<(
        kamu_resources::ResourceTypeDescriptor,
        Option<ResourceQuery>,
    )> {
        canonicals
            .iter()
            .map(|canonical| (descriptor(canonical), None))
            .collect()
    }

    // The arity rule, unit-tested here rather than only end-to-end: one named
    // type keeps its own columns, more than one falls back to generic.
    #[test]
    fn test_scope_for_resolved_types_single_type_keeps_per_type_columns() {
        let scope = ListCommand::scope_for_resolved_types(resolved(&["variablesets"]));
        assert_matches!(scope, ListResourcesScope::ByType(..));
        assert!(!scope.is_generic());
    }

    #[test]
    fn test_scope_for_resolved_types_multiple_types_fall_back_to_generic() {
        let scope =
            ListCommand::scope_for_resolved_types(resolved(&["variablesets", "secretsets"]));
        assert_matches!(scope, ListResourcesScope::Types(_));
        assert!(scope.is_generic());
    }

    // A repeated type still counts as two, so the shape follows what was
    // *asked for* rather than how many distinct types come back.
    #[test]
    fn test_scope_for_resolved_types_repeated_type_is_still_multi_type() {
        let scope =
            ListCommand::scope_for_resolved_types(resolved(&["variablesets", "variablesets"]));
        assert!(scope.is_generic());
    }

    #[test]
    fn test_scope_for_all_types_is_generic() {
        assert!(ListResourcesScope::All(None).is_generic());
    }

    // A query must never change the column shape — only arity may.
    #[test]
    fn test_scope_shape_is_independent_of_query() {
        let with_query = vec![(
            descriptor("variablesets"),
            Some(ResourceQuery::NamePattern("my-%".to_string())),
        )];
        let scope = ListCommand::scope_for_resolved_types(with_query);
        assert!(!scope.is_generic(), "a pattern must not change the columns");
    }

    #[test]
    fn test_resolve_mode_datasets() {
        assert_matches!(resolve(&[]), Ok(ListMode::Datasets));
        assert_matches!(resolve(&["datasets"]), Ok(ListMode::Datasets));
        // `datasets` keeps its historic case-insensitivity.
        assert_matches!(resolve(&["DATASETS"]), Ok(ListMode::Datasets));
    }

    #[test]
    fn test_resolve_mode_single_type_without_query() {
        assert_matches!(
            resolve(&["vs"]),
            Ok(ListMode::ResourcesByTypes(types))
                if types == vec![("vs".to_string(), None)]
        );
    }

    #[test]
    fn test_resolve_mode_name_pattern() {
        assert_matches!(
            resolve(&["vs/my-%"]),
            Ok(ListMode::ResourcesByTypes(types))
                if types == vec![(
                    "vs".to_string(),
                    Some(ResourceQuery::NamePattern("my-%".to_string())),
                )]
        );
    }

    #[test]
    fn test_resolve_mode_exact_name_is_a_degenerate_pattern() {
        assert_matches!(
            resolve(&["vs/my-vars"]),
            Ok(ListMode::ResourcesByTypes(types))
                if types == vec![(
                    "vs".to_string(),
                    Some(ResourceQuery::NamePattern("my-vars".to_string())),
                )]
        );
    }

    #[test]
    fn test_resolve_mode_exact_id_within_type() {
        let id = uuid_v4();
        let Ok(ListMode::ResourcesByTypes(types)) = resolve(&[&format!("vs/{id}")]) else {
            panic!("expected a typed listing");
        };
        assert_matches!(
            types.as_slice(),
            [(type_half, Some(ResourceQuery::ExactIds(ids)))]
                if type_half == "vs" && ids[0].to_string() == id
        );
    }

    #[test]
    fn test_resolve_mode_bare_id_spans_all_types() {
        let id = uuid_v4();
        let Ok(ListMode::ResourcesAll(Some(ResourceQuery::ExactIds(ids)))) = resolve(&[&id]) else {
            panic!("expected an all-types listing by ID");
        };
        assert_eq!(ids[0].to_string(), id);
    }

    // Pins `list` to the same UUIDv4 rule as `get`: a parseable non-v4 UUID is
    // an ordinary name, so a looser check would silently change its meaning.
    #[test]
    fn test_resolve_mode_non_v4_uuid_is_a_name() {
        assert_matches!(
            resolve(&["vs/00000000-0000-1000-8000-000000000000"]),
            Ok(ListMode::ResourcesByTypes(types))
                if matches!(&types[0].1, Some(ResourceQuery::NamePattern(_)))
        );
    }

    #[test]
    fn test_resolve_mode_all_types() {
        assert_matches!(resolve(&["%"]), Ok(ListMode::ResourcesAll(None)));
        assert_matches!(
            resolve(&["%/my-%"]),
            Ok(ListMode::ResourcesAll(Some(ResourceQuery::NamePattern(p))))
                if p == "my-%"
        );
    }

    #[test]
    fn test_resolve_mode_multiple_types() {
        assert_matches!(
            resolve(&["vs/a-%", "ss/b-%"]),
            Ok(ListMode::ResourcesByTypes(types)) if types.len() == 2
        );
        // A repeated type is allowed; results are deduplicated downstream.
        assert_matches!(
            resolve(&["vs/a-%", "vs/b-%"]),
            Ok(ListMode::ResourcesByTypes(types)) if types.len() == 2
        );
    }

    #[test]
    fn test_resolve_mode_rejects_mixing_datasets_with_resources() {
        assert_matches!(resolve(&["datasets", "vs"]), Err(_));
        assert_matches!(resolve(&["datasets/my-%"]), Err(_));
    }

    // `%` already spans every type, so pairing it with a narrower selector is a
    // contradiction rather than a union.
    #[test]
    fn test_resolve_mode_rejects_any_selector_with_other_selectors() {
        assert_matches!(resolve(&["%/a-%", "vs/b-%"]), Err(_));
        assert_matches!(resolve(&["%", "vs"]), Err(_));
    }

    #[test]
    fn test_resolve_mode_rejects_malformed_selectors() {
        for target in ["vs/a/b", "/my-vars", "vs/"] {
            assert_matches!(resolve(&[target]), Err(_), "{target} should be rejected");
        }
    }
}

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ResolvedResourceLabelFilter,
    ResourceExtensionKind,
    ResourceLabelFilterExpr,
    ResourceLabelFilterExprParser,
    ResourceLabelFilterInput,
    ResourceLabelFilterPredicate,
    ResourceSchemaId,
    TypeRef,
};
use kamu_resources_services::ResourceExtensionSchemaResolver;

use crate::{ResourceInvalidLabelFilterError, ResourceLabelFilterProblemCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves an authored label filter into its canonical form.
pub(crate) fn resolve_label_filter(
    resolver: &ResourceExtensionSchemaResolver,
    label_filter: Option<ResourceLabelFilterInput>,
    resource_schema: &ResourceSchemaId,
) -> Result<ResolvedResourceLabelFilter, ResourceInvalidLabelFilterError> {
    let Some(label_filter) = label_filter else {
        return Ok(ResolvedResourceLabelFilter::default());
    };

    let parsed = ResourceLabelFilterExprParser::parse(label_filter.entries)?;

    let resolved = resolve_expr(resolver, &parsed, resource_schema)?;

    ensure_executable(&resolved)?;

    Ok(resolved)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Rejects a resolved filter that no repository backend can evaluate yet.
fn ensure_executable(
    resolved: &ResolvedResourceLabelFilter,
) -> Result<(), ResourceInvalidLabelFilterError> {
    ResourceLabelFilterPredicate::flatten_conjunction(resolved)
        .map(|_| ())
        .map_err(ResourceInvalidLabelFilterError::unsupported_expression)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves one label filter against multiple candidate schemas.
/// Schemas that reject a label key cannot match and are dropped.
pub(crate) fn resolve_label_filter_for_schemas(
    resolver: &ResourceExtensionSchemaResolver,
    label_filter: Option<ResourceLabelFilterInput>,
    resource_schemas: &[ResourceSchemaId],
) -> Result<(Vec<ResourceSchemaId>, ResolvedResourceLabelFilter), ResourceInvalidLabelFilterError> {
    let Some(label_filter) = label_filter else {
        return Ok((resource_schemas.to_vec(), ResolvedResourceLabelFilter::True));
    };

    // Parsing is schema-independent; only key resolution varies per schema.
    let parsed_expr = ResourceLabelFilterExprParser::parse(label_filter.entries)?;

    let mut applicable_schemas = Vec::with_capacity(resource_schemas.len());
    let mut resolved_expr: Option<ResolvedResourceLabelFilter> = None;
    let mut last_error = None;

    for resource_schema in resource_schemas {
        match resolve_expr(resolver, &parsed_expr, resource_schema) {
            Ok(schema_expr) => {
                // Unsupported operators are expression errors, not schema misses.
                ensure_executable(&schema_expr)?;

                if let Some(previous_expr) = &resolved_expr {
                    assert_eq!(
                        previous_expr, &schema_expr,
                        "label filter expression resolved differently for schema \
                         '{resource_schema}' than for a preceding one; per-schema filtering is \
                         not implemented, as no label schema is type-dependent today"
                    );
                } else {
                    resolved_expr = Some(schema_expr);
                }

                applicable_schemas.push(resource_schema.clone());
            }
            Err(err) => last_error = Some(err),
        }
    }

    match (resolved_expr, last_error) {
        (Some(resolved_expr), _) => Ok((applicable_schemas, resolved_expr)),
        // Every candidate schema rejected the filter, so the filter itself is at fault.
        (None, Some(err)) => Err(err),
        // No candidate schemas means no match, not a bad filter.
        (None, None) => Ok((Vec::new(), ResolvedResourceLabelFilter::True)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn resolve_expr(
    resolver: &ResourceExtensionSchemaResolver,
    expr: &ResourceLabelFilterExpr,
    resource_schema: &ResourceSchemaId,
) -> Result<ResolvedResourceLabelFilter, ResourceInvalidLabelFilterError> {
    match expr {
        ResourceLabelFilterExpr::True => Ok(ResolvedResourceLabelFilter::True),

        ResourceLabelFilterExpr::Eq { key, value } => {
            let (key, value) = resolve_eq_entry(resolver, key, value, resource_schema)?;
            Ok(ResolvedResourceLabelFilter::Eq { key, value })
        }

        ResourceLabelFilterExpr::And(children) => {
            let resolved = children
                .iter()
                .map(|child| resolve_expr(resolver, child, resource_schema))
                .collect::<Result<Vec<_>, _>>()?;

            ensure_no_duplicate_keys(&resolved)?;

            Ok(ResolvedResourceLabelFilter::And(resolved))
        }

        ResourceLabelFilterExpr::Not(inner) => Ok(ResolvedResourceLabelFilter::Not(Box::new(
            resolve_expr(resolver, inner, resource_schema)?,
        ))),

        ResourceLabelFilterExpr::Or(branches) => {
            let resolved = branches
                .iter()
                .map(|branch| resolve_expr(resolver, branch, resource_schema))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ResolvedResourceLabelFilter::Or(resolved))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Rejects a key that canonicalizes to the same `TypeRef` twice among the
/// **siblings of one conjunction**, where `a=x AND a=y` is unsatisfiable and
/// almost certainly an authoring mistake.
///
/// Deliberately scoped to a single level: distinct `$or` branches may each
/// legitimately test the same key, and a key repeated across nesting levels is
/// not contradictory.
fn ensure_no_duplicate_keys(
    resolved: &[ResolvedResourceLabelFilter],
) -> Result<(), ResourceInvalidLabelFilterError> {
    let mut keys: Vec<&TypeRef> = resolved
        .iter()
        .filter_map(|child| match child {
            ResolvedResourceLabelFilter::Eq { key, .. } => Some(key),
            _ => None,
        })
        .collect();

    keys.sort();

    for pair in keys.windows(2) {
        if pair[0] == pair[1] {
            return Err(ResourceInvalidLabelFilterError {
                code: ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization,
                message: format!(
                    "label filter key '{}' is authored more than once after canonicalization",
                    pair[0]
                ),
            });
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn resolve_eq_entry(
    resolver: &ResourceExtensionSchemaResolver,
    key: &str,
    value: &serde_json::Value,
    resource_schema: &ResourceSchemaId,
) -> Result<(TypeRef, String), ResourceInvalidLabelFilterError> {
    let type_ref: TypeRef = key
        .parse()
        .map_err(|err| ResourceInvalidLabelFilterError::invalid_key(key, err))?;

    let Some(value) = value.as_str() else {
        return Err(ResourceInvalidLabelFilterError::non_string_value(&type_ref));
    };

    let resolution = resolver.resolve_key(
        ResourceExtensionKind::Label,
        &type_ref,
        &serde_json::Value::String(value.to_owned()),
        resource_schema,
    )?;

    Ok((resolution.canonical_key, value.to_owned()))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kamu_resources::RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI;
    use pretty_assertions::assert_eq;

    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn resolver() -> std::sync::Arc<ResourceExtensionSchemaResolver> {
        let mut builder = dill::CatalogBuilder::new();
        kamu_resources_services::register_dependencies(&mut builder);

        let catalog = builder.build();
        catalog.get_one().unwrap()
    }

    fn schema(name: &str) -> ResourceSchemaId {
        ResourceSchemaId::parse(&format!(
            "https://example.com/schemas/config/v1alpha1/{name}"
        ))
        .unwrap()
    }

    fn filter_of(pairs: &[(&str, &str)]) -> ResourceLabelFilterInput {
        ResourceLabelFilterInput {
            entries: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), serde_json::json!(v)))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    fn uri_key(uri: &str) -> TypeRef {
        TypeRef::Uri(kamu_resources::TypeUri::new_unchecked(uri))
    }

    fn name_key(name: &str) -> TypeRef {
        TypeRef::Name(name.parse().unwrap())
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn no_filter_keeps_every_schema_and_resolves_to_true() {
        let schemas = vec![schema("Widget"), schema("Gadget")];

        let (applicable, resolved) =
            resolve_label_filter_for_schemas(&resolver(), None, &schemas).unwrap();

        assert_eq!(applicable, schemas);
        assert_eq!(resolved, ResolvedResourceLabelFilter::True);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn empty_schema_list_with_a_filter_yields_no_candidates() {
        let (applicable, resolved) = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[("environment", "prod")])),
            &[],
        )
        .unwrap();

        assert!(applicable.is_empty());
        assert_eq!(resolved, ResolvedResourceLabelFilter::True);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn empty_schema_list_without_a_filter_yields_no_candidates() {
        let (applicable, resolved) =
            resolve_label_filter_for_schemas(&resolver(), None, &[]).unwrap();

        assert!(applicable.is_empty());
        assert_eq!(resolved, ResolvedResourceLabelFilter::True);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn single_label_resolves_to_a_bare_equality() {
        let schemas = vec![schema("Widget")];

        let (applicable, resolved) = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[("environment", "prod")])),
            &schemas,
        )
        .unwrap();

        assert_eq!(applicable, schemas);
        assert_eq!(
            resolved,
            ResolvedResourceLabelFilter::Eq {
                key: uri_key(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
                value: "prod".to_string(),
            }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn two_independent_labels_resolve_into_one_conjunction() {
        let schemas = vec![schema("Widget")];

        let (_, resolved) = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[("environment", "prod"), ("team", "data")])),
            &schemas,
        )
        .unwrap();

        assert_eq!(
            resolved,
            ResolvedResourceLabelFilter::And(vec![
                ResolvedResourceLabelFilter::Eq {
                    key: uri_key(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
                    value: "prod".to_string(),
                },
                ResolvedResourceLabelFilter::Eq {
                    key: name_key("team"),
                    value: "data".to_string(),
                },
            ])
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn built_in_label_resolves_identically_across_schemas() {
        let schemas = vec![schema("Widget"), schema("Gadget"), schema("Doohickey")];

        let (applicable, resolved) = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[("environment", "prod")])),
            &schemas,
        )
        .unwrap();

        assert_eq!(applicable, schemas);
        assert_eq!(
            resolved,
            ResolvedResourceLabelFilter::Eq {
                key: uri_key(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
                value: "prod".to_string(),
            }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn free_form_label_stays_uncanonicalized_across_schemas() {
        let schemas = vec![schema("Widget"), schema("Gadget")];

        let (applicable, resolved) = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[("team", "data")])),
            &schemas,
        )
        .unwrap();

        assert_eq!(applicable, schemas);
        assert_eq!(
            resolved,
            ResolvedResourceLabelFilter::Eq {
                key: name_key("team"),
                value: "data".to_string(),
            }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn unknown_uri_key_is_rejected_for_every_schema() {
        let schemas = vec![schema("Widget"), schema("Gadget")];

        let err = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[(
                "https://example.com/schemas/labels/v1alpha1/NotRegistered",
                "x",
            )])),
            &schemas,
        )
        .unwrap_err();

        assert_eq!(
            err.code,
            ResourceLabelFilterProblemCode::ResourceExtensionSchema
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn unsupported_operator_fails_outright() {
        let schemas = vec![schema("Widget"), schema("Gadget")];

        let label_filter = ResourceLabelFilterInput {
            entries: BTreeMap::from([(
                "$or".to_string(),
                serde_json::json!([{"environment": "prod"}, {"environment": "staging"}]),
            )]),
        };

        let err = resolve_label_filter_for_schemas(&resolver(), Some(label_filter), &schemas)
            .unwrap_err();

        assert_eq!(
            err.code,
            ResourceLabelFilterProblemCode::UnsupportedExpression
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn duplicate_key_after_canonicalization_is_rejected() {
        let schemas = vec![schema("Widget")];

        // Short name and canonical URI collapse to the same key.
        let err = resolve_label_filter_for_schemas(
            &resolver(),
            Some(filter_of(&[
                ("environment", "prod"),
                (RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "staging"),
            ])),
            &schemas,
        )
        .unwrap_err();

        assert_eq!(
            err.code,
            ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

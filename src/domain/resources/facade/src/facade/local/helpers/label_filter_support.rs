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
///
/// The whole expression tree is resolved, including `$not`/`$or` branches —
/// resolution is about canonicalizing keys and validating values, which is
/// meaningful regardless of the operator wrapping a leaf. Whether an operator
/// can actually be *executed* is decided further down, by
/// [`ResourceLabelFilterPredicate::flatten_conjunction`] at the repository
/// boundary, so that growing support is a backend-local change.
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
///
/// The check is delegated to
/// [`ResourceLabelFilterPredicate::flatten_conjunction`] so that the set of
/// supported operators is defined in exactly one place. It runs here, at the
/// facade edge, purely so the caller gets a typed
/// [`ResourceLabelFilterProblemCode::UnsupportedExpression`] instead of an
/// opaque `InternalError` from deep inside a repository. Once the backends can
/// evaluate an operator, `flatten_conjunction` stops rejecting it and this
/// check lets it through unchanged.
fn ensure_executable(
    resolved: &ResolvedResourceLabelFilter,
) -> Result<(), ResourceInvalidLabelFilterError> {
    ResourceLabelFilterPredicate::flatten_conjunction(resolved)
        .map(|_| ())
        .map_err(ResourceInvalidLabelFilterError::unsupported_expression)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the authored filter expression — the whole `LabelFilter` object,
/// however many entries and nested operators it carries — against several
/// candidate schemas at once, for the multi-type selectors that `get`/`delete`
/// expand (`kamu get '%/%' -l env=prod,team=data`).
///
/// Returns the schemas that can still match, paired with the one resolved
/// expression tree to apply to all of them.
///
/// Two behaviours are deliberate:
///
/// - A schema whose extension registry rejects the key is **dropped** rather
///   than failing the whole query: a label that is inapplicable to a type just
///   means no resource of that type can match. If every schema rejects it, the
///   error is surfaced, since that indicates a genuinely bad filter.
/// - The resolved filters are asserted to be **identical** across schemas. No
///   label schema resolves differently per resource type today — built-in
///   labels apply to every type and unregistered keys stay free-form — so the
///   divergent case is unreachable. Supporting it would mean OR-ing per-schema
///   predicates in every backend, which is left unimplemented rather than
///   written blind and never exercised.
pub(crate) fn resolve_label_filter_for_schemas(
    resolver: &ResourceExtensionSchemaResolver,
    label_filter: Option<ResourceLabelFilterInput>,
    resource_schemas: &[ResourceSchemaId],
) -> Result<(Vec<ResourceSchemaId>, ResolvedResourceLabelFilter), ResourceInvalidLabelFilterError> {
    let Some(label_filter) = label_filter else {
        return Ok((resource_schemas.to_vec(), ResolvedResourceLabelFilter::True));
    };

    // Parsing is schema-independent, so it happens once up front; only
    // resolution — canonicalizing keys against a schema's extension registry —
    // varies per schema. All authored entries become a single tree here: two
    // labels parse to one `And([Eq, Eq])` root, not two separate filters.
    let parsed_expr = ResourceLabelFilterExprParser::parse(label_filter.entries)?;

    let mut applicable_schemas = Vec::with_capacity(resource_schemas.len());
    let mut resolved_expr: Option<ResolvedResourceLabelFilter> = None;
    let mut last_error = None;

    for resource_schema in resource_schemas {
        match resolve_expr(resolver, &parsed_expr, resource_schema) {
            Ok(schema_expr) => {
                // An unsupported operator is a property of the expression, not
                // of any one schema, so it fails outright instead of merely
                // excluding this schema from the candidate set.
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
        // Every candidate schema rejected the filter, so the filter itself is
        // at fault.
        (None, Some(err)) => Err(err),
        // No candidate schemas at all. There is nothing the filter could be
        // wrong about, so this is not an error: callers pass the empty schema
        // list down, and the repositories already return an empty result for
        // it. The filter value is irrelevant when no schema can match.
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
        let catalog =
            kamu_resources_services::build_catalog_with_resource_extension_schema_registry(builder)
                .unwrap();

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
        // A selector matching no resource type is not a filter error: the
        // repositories already return an empty result for an empty schema
        // list, so resolution must not fail (nor panic) here.
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

        // Both labels live in a single resolved tree: `environment`
        // canonicalizes to its schema URI, `team` stays free-form.
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
        // `environment` is registered as `AnyResource`, so every candidate
        // type yields the same tree and the uniform path is taken.
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

        // No schema can resolve an unregistered URI, so the filter itself is
        // at fault and the error surfaces rather than emptying the candidates.
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

        // Not merely an inapplicable-per-schema outcome: `$or` is a property
        // of the expression, so it must not be reported as "no schema matched".
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

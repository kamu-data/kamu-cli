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

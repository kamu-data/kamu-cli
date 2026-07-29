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
    ResourceSchemaId,
    TypeRef,
};
use kamu_resources_services::ResourceExtensionSchemaResolver;

use crate::{ResourceInvalidLabelFilterError, ResourceLabelFilterProblemCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resolve_label_filter(
    resolver: &ResourceExtensionSchemaResolver,
    label_filter: Option<ResourceLabelFilterInput>,
    resource_schema: &ResourceSchemaId,
) -> Result<ResolvedResourceLabelFilter, ResourceInvalidLabelFilterError> {
    let Some(label_filter) = label_filter else {
        return Ok(ResolvedResourceLabelFilter::default());
    };

    let parsed = ResourceLabelFilterExprParser::parse(label_filter.entries)?;

    let mut entries = Vec::with_capacity(parsed.len());
    for expr in parsed {
        match expr {
            ResourceLabelFilterExpr::Eq { key, value } => {
                entries.push(resolve_eq_entry(resolver, &key, &value, resource_schema)?);
            }
            ResourceLabelFilterExpr::Not(_) => {
                return Err(ResourceInvalidLabelFilterError::unsupported_expression(
                    "'$not' is not supported yet",
                ));
            }
            ResourceLabelFilterExpr::Or(_) => {
                return Err(ResourceInvalidLabelFilterError::unsupported_expression(
                    "'$or' is not supported yet",
                ));
            }
        }
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in entries.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(ResourceInvalidLabelFilterError {
                code: ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization,
                message: format!(
                    "label filter key '{}' is authored more than once after canonicalization",
                    pair[0].0
                ),
            });
        }
    }

    Ok(ResolvedResourceLabelFilter { entries })
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

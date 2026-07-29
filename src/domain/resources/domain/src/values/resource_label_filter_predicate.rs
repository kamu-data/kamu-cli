// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResolvedResourceLabelFilter, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Raised when a resolved label filter uses a boolean operator that no
/// repository backend can evaluate yet.
///
/// This is the **only** definition of the supported-operator boundary: the
/// filter model, the facade, and every repository implementation are all able
/// to carry `$not`/`$or`, and it is
/// [`ResourceLabelFilterPredicate::flatten_conjunction`] alone that decides
/// they cannot yet be executed. Growing support therefore means teaching the
/// backends a richer predicate builder, not re-shaping any type in between.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
#[error("label filter operator '{operator}' is not supported yet")]
pub struct UnsupportedLabelFilterOperatorError {
    pub operator: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Translates a [`ResolvedResourceLabelFilter`] into the predicate shape the
/// repository backends can execute, and owns the definition of which boolean
/// operators are supported.
pub struct ResourceLabelFilterPredicate;

impl ResourceLabelFilterPredicate {
    /// Flattens a resolved filter into the plain `(key, value)` conjunction
    /// that repositories translate into correlated `EXISTS` predicates over
    /// `resource_labels_projection`.
    ///
    /// Nested [`ResolvedResourceLabelFilter::And`] nodes are flattened, since a
    /// conjunction of conjunctions is still a conjunction.
    /// [`ResolvedResourceLabelFilter::Not`]/[`ResolvedResourceLabelFilter::Or`]
    /// yield [`UnsupportedLabelFilterOperatorError`] — callers surface this as
    /// an unsupported-expression problem rather than silently dropping the
    /// predicate, which would over-match.
    pub fn flatten_conjunction(
        filter: &ResolvedResourceLabelFilter,
    ) -> Result<Vec<(&TypeRef, &str)>, UnsupportedLabelFilterOperatorError> {
        let mut pairs = Vec::new();
        Self::collect_conjunction(filter, &mut pairs)?;

        Ok(pairs)
    }

    fn collect_conjunction<'a>(
        filter: &'a ResolvedResourceLabelFilter,
        pairs: &mut Vec<(&'a TypeRef, &'a str)>,
    ) -> Result<(), UnsupportedLabelFilterOperatorError> {
        match filter {
            ResolvedResourceLabelFilter::True => Ok(()),

            ResolvedResourceLabelFilter::Eq { key, value } => {
                pairs.push((key, value.as_str()));
                Ok(())
            }

            ResolvedResourceLabelFilter::And(children) => {
                for child in children {
                    Self::collect_conjunction(child, pairs)?;
                }
                Ok(())
            }

            ResolvedResourceLabelFilter::Not(_) => {
                Err(UnsupportedLabelFilterOperatorError { operator: "$not" })
            }

            ResolvedResourceLabelFilter::Or(_) => {
                Err(UnsupportedLabelFilterOperatorError { operator: "$or" })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn eq(key: &str, value: &str) -> ResolvedResourceLabelFilter {
        ResolvedResourceLabelFilter::Eq {
            key: TypeRef::Name(key.parse().unwrap()),
            value: value.to_string(),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn flattens_true_to_no_pairs() {
        let pairs =
            ResourceLabelFilterPredicate::flatten_conjunction(&ResolvedResourceLabelFilter::True)
                .unwrap();

        assert!(pairs.is_empty());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn flattens_single_equality() {
        let filter = eq("environment", "prod");

        let pairs = ResourceLabelFilterPredicate::flatten_conjunction(&filter).unwrap();

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, "prod");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn flattens_nested_conjunctions() {
        let filter = ResolvedResourceLabelFilter::And(vec![
            eq("a", "x"),
            ResolvedResourceLabelFilter::And(vec![eq("b", "y"), eq("c", "z")]),
        ]);

        let pairs = ResourceLabelFilterPredicate::flatten_conjunction(&filter).unwrap();

        assert_eq!(
            pairs.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_not_operator() {
        let filter = ResolvedResourceLabelFilter::Not(Box::new(eq("a", "x")));

        assert_matches!(
            ResourceLabelFilterPredicate::flatten_conjunction(&filter),
            Err(UnsupportedLabelFilterOperatorError { operator: "$not" })
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_or_operator() {
        let filter = ResolvedResourceLabelFilter::Or(vec![eq("a", "x"), eq("b", "y")]);

        assert_matches!(
            ResourceLabelFilterPredicate::flatten_conjunction(&filter),
            Err(UnsupportedLabelFilterOperatorError { operator: "$or" })
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_operator_nested_inside_a_conjunction() {
        let filter = ResolvedResourceLabelFilter::And(vec![
            eq("a", "x"),
            ResolvedResourceLabelFilter::Not(Box::new(eq("b", "y"))),
        ]);

        assert_matches!(
            ResourceLabelFilterPredicate::flatten_conjunction(&filter),
            Err(UnsupportedLabelFilterOperatorError { operator: "$not" })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

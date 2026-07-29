// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::{ResourceLabelFilterExpr, ResourceLabelFilterExprParseError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parses the raw entries of a
/// [`ResourceLabelFilterInput`](crate::ResourceLabelFilterInput) into the
/// [`ResourceLabelFilterExpr`] tree shape, without resolving or validating
/// any key/value (that happens one layer up, only for
/// [`ResourceLabelFilterExpr::Eq`] leaves).
pub struct ResourceLabelFilterExprParser;

impl ResourceLabelFilterExprParser {
    /// Entries stay an implicit AND at every level (top-level and nested
    /// under `$not`/`$or`), same as the ODF `LabelFilter` schema's object
    /// shape. An empty object yields [`ResourceLabelFilterExpr::True`], and a
    /// single entry is returned unwrapped rather than in a one-element
    /// [`ResourceLabelFilterExpr::And`].
    pub fn parse(
        entries: BTreeMap<String, serde_json::Value>,
    ) -> Result<ResourceLabelFilterExpr, ResourceLabelFilterExprParseError> {
        let mut parsed = entries
            .into_iter()
            .map(Self::parse_entry)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(match parsed.len() {
            0 => ResourceLabelFilterExpr::True,
            1 => parsed.pop().unwrap(),
            _ => ResourceLabelFilterExpr::And(parsed),
        })
    }

    fn parse_entry(
        (key, value): (String, serde_json::Value),
    ) -> Result<ResourceLabelFilterExpr, ResourceLabelFilterExprParseError> {
        match key.as_str() {
            "$not" => Self::parse_not(&value),
            "$or" => Self::parse_or(&value),
            _ if key.starts_with('$') => {
                Err(ResourceLabelFilterExprParseError::UnknownOperator(key))
            }
            _ => Ok(ResourceLabelFilterExpr::Eq { key, value }),
        }
    }

    fn parse_not(
        value: &serde_json::Value,
    ) -> Result<ResourceLabelFilterExpr, ResourceLabelFilterExprParseError> {
        let nested = Self::parse_filter_object(value)
            .map_err(|v| ResourceLabelFilterExprParseError::NotExpectsObject(v.to_string()))?;

        Ok(ResourceLabelFilterExpr::Not(Box::new(nested)))
    }

    fn parse_or(
        value: &serde_json::Value,
    ) -> Result<ResourceLabelFilterExpr, ResourceLabelFilterExprParseError> {
        let serde_json::Value::Array(items) = value else {
            return Err(ResourceLabelFilterExprParseError::OrExpectsArrayOfObjects(
                value.to_string(),
            ));
        };

        // Each array element is one disjunction branch. Branches are kept as
        // separate nodes — flattening their entries into a single list would
        // silently turn `[{a, b}, {c}]` into `a OR b OR c`.
        let branches = items
            .iter()
            .map(|item| {
                Self::parse_filter_object(item).map_err(|v| {
                    ResourceLabelFilterExprParseError::OrExpectsArrayOfObjects(v.to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResourceLabelFilterExpr::Or(branches))
    }

    /// Parses a nested filter object (the value under `$not`, or one element
    /// of the `$or` array). Returns the original value on error so callers can
    /// report it with their own operator-specific message.
    fn parse_filter_object(
        value: &serde_json::Value,
    ) -> Result<ResourceLabelFilterExpr, serde_json::Value> {
        let serde_json::Value::Object(object) = value else {
            return Err(value.clone());
        };

        let entries = object
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>();

        Self::parse(entries).map_err(|_| value.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn eq(key: &str, value: &str) -> ResourceLabelFilterExpr {
        ResourceLabelFilterExpr::Eq {
            key: key.to_string(),
            value: serde_json::json!(value),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_empty_object_as_true() {
        let parsed = ResourceLabelFilterExprParser::parse(BTreeMap::new()).unwrap();

        assert_eq!(parsed, ResourceLabelFilterExpr::True);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_plain_equality() {
        let entries = BTreeMap::from([("environment".to_string(), serde_json::json!("prod"))]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(parsed, eq("environment", "prod"));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_multiple_top_level_entries_as_conjunction() {
        let entries = BTreeMap::from([
            ("a".to_string(), serde_json::json!("x")),
            ("b".to_string(), serde_json::json!("y")),
        ]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::And(vec![eq("a", "x"), eq("b", "y")])
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_not_object() {
        let entries = BTreeMap::from([(
            "$not".to_string(),
            serde_json::json!({"environment": "prod"}),
        )]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Not(Box::new(eq("environment", "prod")))
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_not_object_with_multiple_entries_as_conjunction() {
        let entries =
            BTreeMap::from([("$not".to_string(), serde_json::json!({"a": "x", "b": "y"}))]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        // `NOT (a=x AND b=y)`, not `(NOT a=x) AND (NOT b=y)`.
        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Not(Box::new(ResourceLabelFilterExpr::And(vec![
                eq("a", "x"),
                eq("b", "y"),
            ])))
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_nested_not_inside_not() {
        let entries = BTreeMap::from([(
            "$not".to_string(),
            serde_json::json!({"$not": {"environment": "prod"}}),
        )]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Not(Box::new(ResourceLabelFilterExpr::Not(Box::new(eq(
                "environment",
                "prod"
            )))))
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_or_array() {
        let entries = BTreeMap::from([(
            "$or".to_string(),
            serde_json::json!([{"name": "foo"}, {"name": "bar"}]),
        )]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Or(vec![eq("name", "foo"), eq("name", "bar")])
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_or_branches_preserving_grouping() {
        let entries = BTreeMap::from([(
            "$or".to_string(),
            serde_json::json!([{"a": "x", "b": "y"}, {"c": "z"}]),
        )]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        // `(a=x AND b=y) OR c=z` — the first branch must stay one node rather
        // than being flattened into `a=x OR b=y OR c=z`.
        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Or(vec![
                ResourceLabelFilterExpr::And(vec![eq("a", "x"), eq("b", "y")]),
                eq("c", "z"),
            ])
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_nested_not_inside_or_branch() {
        let entries = BTreeMap::from([(
            "$or".to_string(),
            serde_json::json!([{"$not": {"a": "x"}}, {"b": "y"}]),
        )]);

        let parsed = ResourceLabelFilterExprParser::parse(entries).unwrap();

        assert_eq!(
            parsed,
            ResourceLabelFilterExpr::Or(vec![
                ResourceLabelFilterExpr::Not(Box::new(eq("a", "x"))),
                eq("b", "y"),
            ])
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_or_given_a_non_array() {
        let entries = BTreeMap::from([("$or".to_string(), serde_json::json!({"a": "x"}))]);

        assert_matches!(
            ResourceLabelFilterExprParser::parse(entries),
            Err(ResourceLabelFilterExprParseError::OrExpectsArrayOfObjects(
                _
            ))
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_not_given_a_non_object() {
        let entries = BTreeMap::from([("$not".to_string(), serde_json::json!("not-an-object"))]);

        assert_matches!(
            ResourceLabelFilterExprParser::parse(entries),
            Err(ResourceLabelFilterExprParseError::NotExpectsObject(_))
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_unknown_operator_key() {
        let entries = BTreeMap::from([("$foo".to_string(), serde_json::json!("bar"))]);

        assert_matches!(
            ResourceLabelFilterExprParser::parse(entries),
            Err(ResourceLabelFilterExprParseError::UnknownOperator(key)) if key == "$foo"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

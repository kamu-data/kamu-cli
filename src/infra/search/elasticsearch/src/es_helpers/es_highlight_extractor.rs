// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::SearchHighlight;

use super::{FIELD_SUFFIX_KEYWORD, FIELD_SUFFIX_NGRAM, FIELD_SUFFIX_SUBSTR, FIELD_SUFFIX_TOKENS};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchHighlightExtractor {}

impl ElasticsearchHighlightExtractor {
    pub fn extract_highlights(highlight_json: &serde_json::Value) -> Option<Vec<SearchHighlight>> {
        if let Some(highlight_obj) = highlight_json.as_object() {
            let mut highlights = Vec::new();
            let mut seen_fields = std::collections::HashSet::new();

            for (field, fragments_value) in highlight_obj {
                // Strip internal field suffix if present
                let normalized_field = Self::strip_internal_suffix(field);

                // Skip if we've already seen this normalized field
                if !seen_fields.insert(normalized_field.clone()) {
                    continue;
                }

                // Extract fragments: we should normally be getting just 1 fragment per field,
                // as we set  "number_of_fragments": 1 in the ES query
                if let Some(fragments_array) = fragments_value.as_array() {
                    let first_fragment = fragments_array
                        .first()
                        .and_then(|v| v.as_str().map(ToString::to_string))
                        .unwrap_or_default();

                    highlights.push(SearchHighlight {
                        field: normalized_field,
                        best_fragment: first_fragment,
                    });
                }
            }

            if highlights.is_empty() {
                None
            } else {
                Some(highlights)
            }
        } else {
            None
        }
    }

    const INTERNAL_FIELD_SUFFIXES: &[&str] = &[
        FIELD_SUFFIX_KEYWORD,
        FIELD_SUFFIX_NGRAM,
        FIELD_SUFFIX_SUBSTR,
        FIELD_SUFFIX_TOKENS,
    ];

    fn strip_internal_suffix(field: &str) -> String {
        for suffix in Self::INTERNAL_FIELD_SUFFIXES {
            let suffix_pattern = format!(".{suffix}");
            if let Some(prefix) = field.strip_suffix(&suffix_pattern) {
                return prefix.to_string();
            }
        }
        field.to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_highlights_normalizes_and_deduplicates_fields() {
        let value = serde_json::json!({
            "name.ngram": ["<em>pow</em>er"],
            "name.keyword": ["power"],
            "description": ["clean <em>energy</em>"],
        });

        let result = ElasticsearchHighlightExtractor::extract_highlights(&value)
            .expect("highlights must be present");

        assert_eq!(result.len(), 2);
        let map: std::collections::HashMap<_, _> = result
            .into_iter()
            .map(|h| (h.field, h.best_fragment))
            .collect();
        let name_fragment = map.get("name").expect("name highlight");
        assert!(name_fragment == "<em>pow</em>er" || name_fragment == "power");
        assert_eq!(
            map.get("description").expect("description highlight"),
            "clean <em>energy</em>"
        );
    }

    #[test]
    fn test_extract_highlights_skips_non_array_fragments() {
        let value = serde_json::json!({
            "name": "bad-shape",
            "description": [],
        });

        let result = ElasticsearchHighlightExtractor::extract_highlights(&value)
            .expect("highlights must be present");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].field, "description");
        assert_eq!(result[0].best_fragment, "");
    }

    #[test]
    fn test_extract_highlights_returns_none_for_non_object() {
        let value = serde_json::json!(["unexpected"]);
        assert!(ElasticsearchHighlightExtractor::extract_highlights(&value).is_none());
    }

    #[test]
    fn test_strip_internal_suffix() {
        let test_cases = vec![
            ("title.keyword", "title"),
            ("title.ngram", "title"),
            ("title.substr", "title"),
            ("title.tokens", "title"),
            ("title", "title"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                ElasticsearchHighlightExtractor::strip_internal_suffix(input),
                expected
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

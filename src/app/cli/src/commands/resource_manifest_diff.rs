// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the before/after canonical manifests of an apply as a compact,
//! colored diff.
//!
//! The backend deliberately ships whole canonical documents rather than a list
//! of field-level changes, so deciding *what* changed and *how to show it* is
//! this module's job. It works in two stages:
//!
//! 1. **Structured detect** — walk both JSON documents together and collect the
//!    paths whose values differ. This is what keeps the output small: an
//!    unchanged region is never rendered at all, no matter how large it is.
//! 2. **Text render** — serialize just the changed regions to YAML and diff
//!    those line-wise. YAML because that is what users author, and because its
//!    line-per-scalar shape diffs far more legibly than JSON's braces.
//!
//! Kept free of `OutputConfig`, progress bars, and `self` so it can be tested
//! directly on strings.

use internal_error::{InternalError, ResultIntoInternal};
use similar::{ChangeTag, TextDiff};

use super::common::json_to_yaml_value;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct ManifestDiffOptions {
    /// Emit ANSI color. Callers pass `false` for non-terminal output; `console`
    /// independently strips color when it detects a non-tty or `NO_COLOR`.
    pub colors_enabled: bool,

    /// Unchanged YAML lines to keep around each changed line, within a region.
    pub context_radius: usize,

    /// Indentation applied to every emitted line, so the diff nests under the
    /// per-manifest block the way warnings do.
    pub indent: String,
}

impl Default for ManifestDiffOptions {
    fn default() -> Self {
        Self {
            colors_enabled: true,
            context_radius: 2,
            indent: "  ".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Renders the diff between two canonical manifest documents.
///
/// `before` is `None` when the resource is being created, which renders as an
/// all-additions diff. Returns an empty string when the documents are equal,
/// so callers can treat "no output" as "nothing changed".
pub(crate) fn render_manifest_diff(
    before: Option<&serde_json::Value>,
    after: &serde_json::Value,
    opts: &ManifestDiffOptions,
) -> Result<String, InternalError> {
    if before == Some(after) {
        return Ok(String::new());
    }

    let mut regions = Vec::new();
    collect_changed_regions(String::new(), before, Some(after), &mut regions);

    let mut blocks = Vec::new();

    for region in regions {
        let block = render_region(&region, opts)?;
        if !block.is_empty() {
            blocks.push(block);
        }
    }

    Ok(blocks.join("\n"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One subtree that differs between the two documents.
struct ChangedRegion {
    /// Dotted path of the subtree, e.g. `headers.labels` or `spec.variables`.
    /// Empty only for a whole-document change (a create).
    path: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
}

/// Descends into matching objects so the reported region is as narrow as
/// possible, and stops as soon as the two sides stop being comparable objects.
///
/// Descending is what makes "one label changed inside a big map" report
/// `headers.labels`, not `headers` — and one changed variable report
/// `spec.variables.entries`, not the whole spec.
fn collect_changed_regions(
    path: String,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
    out: &mut Vec<ChangedRegion>,
) {
    if before == after {
        return;
    }

    // Only descend when both sides are objects: for scalars, arrays, or a
    // type change there is no finer structure to attribute the change to.
    if let (
        Some(serde_json::Value::Object(before_map)),
        Some(serde_json::Value::Object(after_map)),
    ) = (before, after)
    {
        let mut keys: Vec<&String> = before_map.keys().chain(after_map.keys()).collect();
        keys.sort_unstable();
        keys.dedup();

        for key in keys {
            // Shorten for display only — lookups must use the stored key.
            let label = display_key(key);
            let child_path = if path.is_empty() {
                label
            } else {
                format!("{path}.{label}")
            };

            collect_changed_regions(child_path, before_map.get(key), after_map.get(key), out);
        }

        return;
    }

    out.push(ChangedRegion {
        path,
        before: before.cloned(),
        after: after.cloned(),
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Shortens a registered extension key to its type name for display.
///
/// Label, annotation, and condition keys are stored as full schema URIs — that
/// URI *is* the identity, so the documents keep it verbatim. But in a path
/// anchor it swamps the line:
///
/// ```text
/// headers.labels.https://kamu.dev/schemas/resource/v1alpha1/labels/Environment:
/// headers.labels.Environment:
/// ```
///
/// Only the anchor is shortened; the diffed content is untouched. Anything that
/// is not a well-formed schema URI — a free-form short name like `env`, or a
/// plain map key — is returned unchanged, so this can never mangle a key it
/// does not understand.
fn display_key(key: &str) -> String {
    if !key.contains("://") {
        return key.to_string();
    }

    // Deliberately not `resource_type_name`: that treats an unparseable URI as
    // a data-integrity error, which is the right call for stored identity but
    // wrong for display — here an odd key should simply render as-is.
    kamu_resources::ResourceSchemaId::parse(key)
        .map(|schema| schema.type_name().to_string())
        .unwrap_or_else(|_| key.to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn render_region(
    region: &ChangedRegion,
    opts: &ManifestDiffOptions,
) -> Result<String, InternalError> {
    let before_text = render_value_as_yaml(region.before.as_ref())?;
    let after_text = render_value_as_yaml(region.after.as_ref())?;

    let diff = TextDiff::from_lines(&before_text, &after_text);

    let mut lines = Vec::new();

    let header = if region.path.is_empty() {
        "manifest".to_string()
    } else {
        region.path.clone()
    };
    lines.push(format!(
        "{}{}:",
        opts.indent,
        style_if(&header, opts.colors_enabled, Styling::Path)
    ));

    let body_indent = format!("{}  ", opts.indent);

    for (group_index, group) in diff
        .grouped_ops(opts.context_radius)
        .into_iter()
        .enumerate()
    {
        // A gap between groups means unchanged lines were skipped.
        if group_index > 0 {
            lines.push(format!(
                "{body_indent}{}",
                style_if("...", opts.colors_enabled, Styling::Skipped)
            ));
        }

        for op in group {
            for change in diff.iter_changes(&op) {
                let (marker, styling) = match change.tag() {
                    ChangeTag::Delete => ('-', Styling::Delete),
                    ChangeTag::Insert => ('+', Styling::Insert),
                    ChangeTag::Equal => (' ', Styling::Equal),
                };

                let text = change.to_string();
                let text = text.trim_end_matches('\n');

                lines.push(format!(
                    "{body_indent}{}",
                    style_if(&format!("{marker} {text}"), opts.colors_enabled, styling)
                ));
            }
        }
    }

    Ok(lines.join("\n"))
}

/// Renders one side of a region. An absent side is the empty document, so a
/// create (or a removed key) shows up as pure additions/deletions rather than
/// as a literal `null` line.
fn render_value_as_yaml(value: Option<&serde_json::Value>) -> Result<String, InternalError> {
    let Some(value) = value else {
        return Ok(String::new());
    };

    serde_yaml::to_string(&json_to_yaml_value(value)).int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
enum Styling {
    Path,
    Delete,
    Insert,
    Equal,
    Skipped,
}

fn style_if(text: &str, colors_enabled: bool, styling: Styling) -> String {
    if !colors_enabled {
        return text.to_string();
    }

    match styling {
        Styling::Path => console::style(text).cyan().bold().to_string(),
        // Deliberately not bold: at volume, bold red/green is hard to read.
        Styling::Delete => console::style(text).red().to_string(),
        Styling::Insert => console::style(text).green().to_string(),
        Styling::Equal => text.to_string(),
        Styling::Skipped => console::style(text).dim().to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn plain_opts() -> ManifestDiffOptions {
        ManifestDiffOptions {
            colors_enabled: false,
            ..Default::default()
        }
    }

    /// A realistic manifest: many labels, a sizeable spec. Used to prove that
    /// diff size tracks what changed, not how big the document is.
    fn big_manifest(team: &str) -> serde_json::Value {
        json!({
            "$schema": "https://kamu.dev/schemas/config/v1alpha1/VariableSet",
            "headers": {
                "name": "my-vars",
                "labels": {
                    "environment": "prod",
                    "team": team,
                    "tier": "gold",
                    "region": "eu-west-1",
                    "owner": "data-platform",
                    "costCenter": "cc-42",
                    "criticality": "high",
                    "compliance": "soc2",
                    "rotation": "monthly",
                    "onCall": "team-data"
                },
                "annotations": {
                    "description": "A variable set with a lot of metadata"
                }
            },
            "spec": {
                "variables": {
                    "A": {"value": "1"}, "B": {"value": "2"}, "C": {"value": "3"},
                    "D": {"value": "4"}, "E": {"value": "5"}, "F": {"value": "6"},
                    "G": {"value": "7"}, "H": {"value": "8"}, "I": {"value": "9"}
                }
            }
        })
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // The acceptance criterion for this diff mechanism: changing one label in a
    // large manifest must produce a handful of lines, not two whole-object
    // dumps. The explicit bound makes a regression fail loudly.
    #[test]
    fn one_label_change_produces_a_small_diff() {
        let before = big_manifest("platform");
        let after = big_manifest("data");

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        let removed = rendered
            .lines()
            .filter(|l| l.trim_start().starts_with("- "))
            .count();
        let added = rendered
            .lines()
            .filter(|l| l.trim_start().starts_with("+ "))
            .count();

        assert_eq!(removed, 1, "exactly one removed line\n{rendered}");
        assert_eq!(added, 1, "exactly one added line\n{rendered}");

        assert!(
            rendered.contains("headers.labels.team"),
            "diff must anchor on the changed path\n{rendered}"
        );
        assert!(
            rendered.contains("- platform") && rendered.contains("+ data"),
            "diff must show the old and new value\n{rendered}"
        );

        // The whole point: output is bounded by the change, not the document.
        let line_count = rendered.lines().count();
        assert!(
            line_count <= 4,
            "a one-label change must stay tiny, got {line_count} lines:\n{rendered}"
        );

        // None of the untouched regions may appear.
        for untouched in ["costCenter", "compliance", "\"9\"", "onCall"] {
            assert!(
                !rendered.contains(untouched),
                "unchanged content `{untouched}` must not be rendered\n{rendered}"
            );
        }
    }

    // Registered label/annotation keys are stored as full schema URIs. Those
    // belong in the document, but in a path anchor they swamp the line.
    #[test]
    fn registered_extension_keys_are_shortened_in_the_path_anchor() {
        const ENV_URI: &str = "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment";

        let before = json!({"headers": {"labels": {ENV_URI: "staging"}}});
        let after = json!({"headers": {"labels": {ENV_URI: "prod"}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(
            rendered.contains("headers.labels.Environment:"),
            "path anchor must use the short type name\n{rendered}"
        );
        assert!(
            !rendered.contains(ENV_URI),
            "the full URI must not appear in the anchor\n{rendered}"
        );
        // The values themselves are untouched.
        assert!(
            rendered.contains("- staging") && rendered.contains("+ prod"),
            "{rendered}"
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // A free-form key is not a URI and must survive verbatim.
    #[test]
    fn free_form_keys_are_left_alone() {
        let before = json!({"headers": {"labels": {"env": "staging"}}});
        let after = json!({"headers": {"labels": {"env": "prod"}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(rendered.contains("headers.labels.env:"), "{rendered}");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Shortening must never mangle a key it cannot parse: a URI-looking string
    // that is not a valid schema URL renders as-is rather than being truncated
    // to a misleading fragment.
    #[test]
    fn unparseable_uri_like_keys_render_verbatim() {
        const ODD: &str = "https://example.com/nope";

        let before = json!({"headers": {"labels": {ODD: "a"}}});
        let after = json!({"headers": {"labels": {ODD: "b"}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(
            rendered.contains(ODD),
            "an unparseable key must render verbatim\n{rendered}"
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Re-spelling a free-form key as its registered URI is a key *move*: the
    // value is unchanged, so the diff shows one key removed and another added —
    // and the added one must read as the short name.
    #[test]
    fn key_move_from_free_form_to_registered_reads_as_two_regions() {
        const ENV_URI: &str = "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment";

        let before = json!({"headers": {"labels": {"env": "prod"}}});
        let after = json!({"headers": {"labels": {ENV_URI: "prod"}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(rendered.contains("headers.labels.env:"), "{rendered}");
        assert!(
            rendered.contains("headers.labels.Environment:"),
            "{rendered}"
        );
        assert!(!rendered.contains(ENV_URI), "{rendered}");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn identical_documents_render_nothing() {
        let doc = big_manifest("platform");

        let rendered = render_manifest_diff(Some(&doc), &doc, &plain_opts()).unwrap();

        assert_eq!(rendered, "");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn create_renders_as_all_additions() {
        let after = json!({
            "$schema": "https://kamu.dev/schemas/config/v1alpha1/VariableSet",
            "headers": {"name": "fresh"},
            "spec": {"variables": {"A": {"value": "1"}}}
        });

        let rendered = render_manifest_diff(None, &after, &plain_opts()).unwrap();

        let content: Vec<&str> = rendered
            .lines()
            .map(str::trim_start)
            .filter(|l| l.starts_with('-') || l.starts_with('+'))
            .collect();

        assert!(!content.is_empty(), "a create must render something");
        assert!(
            content.iter().all(|l| l.starts_with('+')),
            "a create must be all additions, got:\n{rendered}"
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn added_and_removed_keys_are_reported_individually() {
        let before = json!({"spec": {"variables": {"A": {"value": "1"}}}});
        let after = json!({"spec": {"variables": {"B": {"value": "2"}}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        // Each key is its own region, so neither is rendered as a whole-map dump.
        assert!(rendered.contains("spec.variables.A"), "{rendered}");
        assert!(rendered.contains("spec.variables.B"), "{rendered}");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Protects piped/CI output, where escape codes would be noise.
    #[test]
    fn colors_disabled_emits_no_ansi_escapes() {
        let before = big_manifest("platform");
        let after = big_manifest("data");

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(
            !rendered.contains('\u{1b}'),
            "no ANSI escapes when colors are disabled\n{rendered:?}"
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn nested_spec_change_anchors_on_the_narrowest_path() {
        let before = json!({"spec": {"variables": {"A": {"value": "1"}}}});
        let after = json!({"spec": {"variables": {"A": {"value": "2"}}}});

        let rendered = render_manifest_diff(Some(&before), &after, &plain_opts()).unwrap();

        assert!(
            rendered.contains("spec.variables.A.value"),
            "must descend to the changed leaf, not report `spec`\n{rendered}"
        );
    }
}

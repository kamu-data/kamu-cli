// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Keeps the agent harness consistent: skills, their Codex symlinks, the
//! AGENTS.md routing table, the hooks' policy file, and links between docs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SKILLS_DIR: &str = ".claude/skills";
const CODEX_SKILLS_DIR: &str = ".agents/skills";
const POLICY_FILE: &str = ".claude/hooks/governed_paths.json";
const ROUTED_DOCS_DIR: &str = "docs/internal";

static LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[[^\]]*\]\(([^)\s]+)\)").unwrap());
static LINE_ANCHOR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^L\d+").unwrap());
static HEADING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^#{1,6}\s+(.+?)\s*#*\s*$").unwrap());
static HTML_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<!--.*?-->").unwrap());
static MD_LINK_TEXT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]*)\]\([^)]*\)").unwrap());
static HTML_ANCHOR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"<a\s+(?:id|name)="([^"]+)""#).unwrap());
static CODE_SPAN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`([^`]+)`").unwrap());

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn skills_are_declared_routed_and_mirrored_for_codex() {
    let root = repo_root();
    let skills = skill_names(&root);
    let mut errors = Vec::new();

    for name in &skills {
        let content = std::fs::read_to_string(root.join(SKILLS_DIR).join(name).join("SKILL.md"))
            .unwrap_or_default();
        let front_matter = front_matter(&content);
        if front_matter.get("name").map(String::as_str) != Some(name.as_str()) {
            errors.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: front matter `name` must be `{name}`"
            ));
        }
        if front_matter.get("description").is_none_or(String::is_empty) {
            errors.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: front matter needs a `description`"
            ));
        }
    }

    let mut codex = BTreeSet::new();
    for entry in std::fs::read_dir(root.join(CODEX_SKILLS_DIR)).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        let expected = PathBuf::from(format!("../../{SKILLS_DIR}/{name}"));
        match std::fs::read_link(entry.path()) {
            Ok(target) if target == expected => {}
            Ok(target) => errors.push(format!(
                "{CODEX_SKILLS_DIR}/{name}: must link to `{}`, links to `{}`",
                expected.display(),
                target.display()
            )),
            Err(_) => errors.push(format!(
                "{CODEX_SKILLS_DIR}/{name}: must be a relative symlink to `{}`, not a real file",
                expected.display()
            )),
        }
        codex.insert(name);
    }
    for name in skills.difference(&codex) {
        errors.push(format!(
            "{CODEX_SKILLS_DIR}/{name}: missing symlink (`ln -s ../../{SKILLS_DIR}/{name} \
             {CODEX_SKILLS_DIR}/{name}`)"
        ));
    }

    let routed: BTreeSet<String> = routing_table(&root)
        .into_iter()
        .map(|(skill, _)| skill)
        .collect();
    for name in skills.difference(&routed) {
        errors.push(format!(
            "AGENTS.md: skill `{name}` has no row in 'What to load for which task'"
        ));
    }
    for name in routed.difference(&skills) {
        errors.push(format!(
            "AGENTS.md: routes skill `{name}`, which does not exist in {SKILLS_DIR}"
        ));
    }

    assert_no_errors("Skill registry is inconsistent", &errors);
}

#[test]
fn routing_table_guarded_paths_match_hook_policy() {
    let root = repo_root();

    let table: Vec<(String, Vec<String>)> = routing_table(&root)
        .into_iter()
        .filter(|(_, paths)| !paths.is_empty())
        .collect();

    let policy: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(root.join(POLICY_FILE)).unwrap()).unwrap();
    let rules: Vec<(String, Vec<String>)> = policy["skills"]
        .as_array()
        .unwrap()
        .iter()
        .map(|rule| {
            let skill = rule["skill"].as_str().unwrap().to_string();
            let paths = rule["paths"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p.as_str().unwrap().to_string())
                .collect();
            (skill, paths)
        })
        .collect();

    pretty_assertions::assert_eq!(
        table,
        rules,
        "The 'Guarded paths' column in AGENTS.md must list the same rules, in the same order, as \
         {POLICY_FILE}"
    );
}

#[test]
fn doc_links_resolve() {
    let root = repo_root();
    let mut errors = Vec::new();

    for file in linted_docs(&root) {
        let rel = file.strip_prefix(&root).unwrap().display().to_string();
        let content = std::fs::read_to_string(&file).unwrap();
        for (line_no, line) in outside_code_fences(&content) {
            for capture in LINK.captures_iter(line) {
                let link = &capture[1];
                if link.contains("://") || link.starts_with("mailto:") {
                    continue;
                }
                if let Err(e) = check_link(&root, &file, link) {
                    errors.push(format!("./{rel}:{line_no}: `{link}` {e}"));
                }
            }
        }
    }

    assert_no_errors("Broken documentation links", &errors);
}

#[test]
fn every_design_doc_is_routed_from_agents_md() {
    let root = repo_root();
    let agents = std::fs::read_to_string(root.join("AGENTS.md")).unwrap();

    let errors: Vec<String> = markdown_files(&root.join(ROUTED_DOCS_DIR))
        .into_iter()
        .map(|f| f.strip_prefix(&root).unwrap().display().to_string())
        .filter(|rel| !agents.contains(rel.as_str()))
        .map(|rel| {
            format!(
                "{rel}: not routed from AGENTS.md 'What to load for which task' — no session will \
                 find it"
            )
        })
        .collect();

    assert_no_errors("Unrouted design documents", &errors);
}

#[test]
fn self_test_slugs() {
    assert_eq!(slug("Build scope (`-p`)"), "build-scope--p");
    assert_eq!(
        slug("Developer Guide <!-- omit in toc -->"),
        "developer-guide"
    );
    assert_eq!(
        slug("17. Extension points & gotchas"),
        "17-extension-points--gotchas"
    );
    assert_eq!(
        slug("3.3 Evolving messages and consumers"),
        "33-evolving-messages-and-consumers"
    );
    assert_eq!(slug("See [the doc](x.md) now"), "see-the-doc-now");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../")
        .canonicalize()
        .unwrap()
}

fn assert_no_errors(title: &str, errors: &[String]) {
    assert!(errors.is_empty(), "{title}:\n{}\n", errors.join("\n"));
}

fn skill_names(root: &Path) -> BTreeSet<String> {
    std::fs::read_dir(root.join(SKILLS_DIR))
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

fn front_matter(content: &str) -> BTreeMap<String, String> {
    let mut lines = content.lines();
    if lines.next() != Some("---") {
        return BTreeMap::new();
    }
    lines
        .take_while(|l| *l != "---")
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// Rows of the skills table in AGENTS.md: (skill, guarded paths), in order.
fn routing_table(root: &Path) -> Vec<(String, Vec<String>)> {
    let agents = std::fs::read_to_string(root.join("AGENTS.md")).unwrap();
    let section = agents
        .split_once("\n### Skills\n")
        .expect("AGENTS.md must have a '### Skills' section")
        .1;
    let section = section.split("\n#").next().unwrap();

    section
        .lines()
        .filter(|l| l.starts_with('|'))
        .skip(2) // header and separator
        .map(|row| {
            let cells: Vec<&str> = row.trim_matches('|').split('|').map(str::trim).collect();
            assert!(
                cells.len() == 3,
                "AGENTS.md skills table row needs 3 cells: {row}"
            );
            let skill = CODE_SPAN
                .captures(cells[1])
                .unwrap_or_else(|| panic!("skill cell must be a code span: {row}"))[1]
                .to_string();
            let paths = CODE_SPAN
                .captures_iter(cells[2])
                .map(|c| c[1].to_string())
                .collect();
            (skill, paths)
        })
        .collect()
}

fn linted_docs(root: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = ["AGENTS.md", "CLAUDE.md", "DEVELOPER.md"]
        .iter()
        .map(|f| root.join(f))
        .collect();
    files.extend(markdown_files(&root.join(ROUTED_DOCS_DIR)));
    files.extend(
        skill_names(root)
            .iter()
            .map(|s| root.join(SKILLS_DIR).join(s).join("SKILL.md")),
    );
    files
}

fn markdown_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = glob::glob(dir.join("**/*.md").to_str().unwrap())
        .unwrap()
        .map(Result::unwrap)
        .collect();
    files.sort();
    files
}

/// Lines with 1-based numbers, skipping fenced code blocks.
fn outside_code_fences(content: &str) -> Vec<(usize, &str)> {
    let mut in_fence = false;
    let mut lines = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if !in_fence {
            lines.push((i + 1, line));
        }
    }
    lines
}

fn check_link(root: &Path, from: &Path, link: &str) -> Result<(), String> {
    let (path, anchor) = match link.split_once('#') {
        Some((p, a)) => (p, Some(a)),
        None => (link, None),
    };

    let target = if path.is_empty() {
        from.to_path_buf()
    } else if let Some(stripped) = path.strip_prefix('/') {
        root.join(stripped)
    } else {
        from.parent().unwrap().join(path)
    };
    if !target.exists() {
        return Err(format!("points to a missing file `{}`", target.display()));
    }

    let Some(anchor) = anchor else {
        return Ok(());
    };
    if LINE_ANCHOR.is_match(anchor) {
        return Err(
            "pins a line number, which goes stale with the next edit; link a heading".to_string(),
        );
    }
    if target.extension().is_some_and(|e| e == "md") {
        let content = std::fs::read_to_string(&target).unwrap();
        if !heading_slugs(&content).contains(anchor) {
            return Err(format!(
                "names a heading anchor `#{anchor}` that does not exist"
            ));
        }
    }
    Ok(())
}

/// Heading anchors plus explicit `<a id="...">` anchors.
fn heading_slugs(content: &str) -> BTreeSet<String> {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut slugs = BTreeSet::new();
    for (_, line) in outside_code_fences(content) {
        slugs.extend(HTML_ANCHOR.captures_iter(line).map(|c| c[1].to_string()));
        let Some(capture) = HEADING.captures(line) else {
            continue;
        };
        let base = slug(&capture[1]);
        let count = seen.entry(base.clone()).or_insert(0);
        slugs.insert(if *count == 0 {
            base.clone()
        } else {
            format!("{base}-{count}")
        });
        *count += 1;
    }
    slugs
}

/// GitHub's heading anchor: lowercase, punctuation dropped, spaces to hyphens.
fn slug(heading: &str) -> String {
    let text = HTML_COMMENT.replace_all(heading, "");
    let text = MD_LINK_TEXT.replace_all(&text, "$1");
    text.trim()
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            ' ' => Some('-'),
            c if c.is_alphanumeric() || c == '-' || c == '_' => Some(c),
            _ => None,
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use pretty_assertions::assert_eq;
use regex::Regex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const E2E_CLI_DIR: &str = "src/e2e/app/cli";

const STORAGES: [Storage; 2] = [Storage::Sqlite, Storage::Postgres];

static INVOCATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?ms)^(kamu_cli_\w+_e2e_test)!\(\n(.*?)^\);$").unwrap());

static FIXTURE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"fixture\s*=\s*([\w:]+)").unwrap());

static OPTIONS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)options\s*=\s*(.*?),?\s*(?:(?:extra_test_groups|sqlite_only|postgres_only)\s*=|$)",
    )
    .unwrap()
});

static EXTRA_TEST_GROUPS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"extra_test_groups\s*=\s*"([^"]*)""#).unwrap());

static SINGLE_STORAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(sqlite_only|postgres_only)\s*=").unwrap());

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn check_e2e_fixtures_are_wired_for_both_databases_or_marked() {
    let repo_root = &get_repo_root();

    let wirings: Vec<_> = STORAGES
        .into_iter()
        .flat_map(|storage| {
            let pattern = repo_root
                .join(E2E_CLI_DIR)
                .join(storage.dir())
                .join("tests/**/*.rs");
            glob::glob(pattern.to_str().unwrap())
                .unwrap()
                .map(Result::unwrap)
                .flat_map(move |file_path| {
                    let file_path_rel = file_path
                        .strip_prefix(repo_root)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .replace('\\', "/");
                    let text = std::fs::read_to_string(&file_path).unwrap();

                    parse_wirings(storage, &file_path_rel, &text)
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let problems = check_wirings(&wirings);

    assert!(
        problems.is_empty(),
        "E2E fixtures are wired inconsistently between SQLite and Postgres:\n{}\n\nWire a fixture \
         for Postgres when the repository type can change its result, otherwise mark its SQLite \
         wiring `sqlite_only = \"<reason>\"` (or `postgres_only` the other way), see \
         .claude/skills/kamu-cli-e2e-tests/SKILL.md",
        problems.join("\n")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn self_test() {
    let sqlite = indoc::indoc!(
        r##"
        kamu_cli_execute_command_e2e_test!(
            storage = sqlite,
            fixture = repo_tests::test_both,
            options = Options::default().with_multi_tenant(),
            extra_test_groups = "engine"
        );

        kamu_cli_execute_command_e2e_test!(
            storage = sqlite,
            fixture = repo_tests::test_marked,
            sqlite_only = "checks a CLI message",
        );

        kamu_cli_execute_command_e2e_test!(
            storage = sqlite,
            fixture = repo_tests::test_unmarked
        );

        kamu_cli_execute_command_e2e_test!(
            storage = sqlite,
            fixture = repo_tests::test_stale_marker,
            sqlite_only = "no longer true"
        );

        kamu_cli_run_api_server_e2e_test!(
            storage = sqlite,
            fixture = repo_tests::test_options_differ,
            options = Options::default()
                .with_kamu_config(indoc::indoc!(
                    r#"
                    kind: CLIConfig
                    "#
                ))
        );
        "##
    );
    let postgres = indoc::indoc!(
        r#"
        kamu_cli_execute_command_e2e_test!(
            storage = postgres,
            fixture = repo_tests::test_both,
            options = Options::default()
                .with_multi_tenant(),
            extra_test_groups = "engine",
        );

        kamu_cli_execute_command_e2e_test!(
            storage = postgres,
            fixture = repo_tests::test_stale_marker
        );

        kamu_cli_run_api_server_e2e_test!(
            storage = postgres,
            fixture = repo_tests::test_options_differ
        );

        kamu_cli_run_api_server_e2e_test!(
            storage = postgres,
            fixture = repo_tests::test_postgres_only,
            postgres_only = "pages through many flows",
        );
        "#
    );

    let mut wirings = parse_wirings(Storage::Sqlite, "sqlite.rs", sqlite);
    wirings.extend(parse_wirings(Storage::Postgres, "postgres.rs", postgres));

    assert_eq!(
        check_wirings(&wirings),
        vec![
            "./sqlite.rs:14: repo_tests::test_unmarked is wired for SQLite only and not marked \
             `sqlite_only`",
            "./sqlite.rs:19: repo_tests::test_stale_marker is marked `sqlite_only` but is also \
             wired for Postgres at ./postgres.rs:9",
            "./sqlite.rs:25: repo_tests::test_options_differ has different `options` for Postgres \
             at ./postgres.rs:14",
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Storage {
    Sqlite,
    Postgres,
}

impl Storage {
    fn dir(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Sqlite => "SQLite",
            Self::Postgres => "Postgres",
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Sqlite => Self::Postgres,
            Self::Postgres => Self::Sqlite,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct Wiring {
    storage: Storage,
    location: String,
    macro_name: String,
    fixture: String,
    options: String,
    extra_test_groups: String,
    single_storage_marker: Option<String>,
}

/// One entry per wiring macro invocation in a file
fn parse_wirings(storage: Storage, file_path_rel: &str, text: &str) -> Vec<Wiring> {
    INVOCATION
        .captures_iter(text)
        .map(|captures| {
            let start = captures.get(0).unwrap().start();
            let line_num = text[..start].matches('\n').count() + 1;
            let body = &captures[2];

            Wiring {
                storage,
                location: format!("./{file_path_rel}:{line_num}"),
                macro_name: captures[1].to_string(),
                fixture: FIXTURE
                    .captures(body)
                    .unwrap_or_else(|| panic!("./{file_path_rel}:{line_num}: no fixture"))[1]
                    .to_string(),
                options: OPTIONS
                    .captures(body)
                    .map(|c| c[1].split_whitespace().collect())
                    .unwrap_or_default(),
                extra_test_groups: EXTRA_TEST_GROUPS
                    .captures(body)
                    .map(|c| c[1].split_whitespace().collect())
                    .unwrap_or_default(),
                single_storage_marker: SINGLE_STORAGE.captures(body).map(|c| c[1].to_string()),
            }
        })
        .collect()
}

/// Problems in wiring order: unmarked one-database fixtures, stale markers,
/// and twins whose settings differ
fn check_wirings(wirings: &[Wiring]) -> Vec<String> {
    let by_key: BTreeMap<(Storage, &str, &str), &Wiring> = wirings
        .iter()
        .map(|w| ((w.storage, w.macro_name.as_str(), w.fixture.as_str()), w))
        .collect();

    let mut problems = Vec::new();

    for wiring in wirings {
        let other = wiring.storage.other();
        let twin = by_key.get(&(other, wiring.macro_name.as_str(), wiring.fixture.as_str()));
        let marker = format!("{}_only", wiring.storage.dir());

        match (twin, &wiring.single_storage_marker) {
            (None, None) => problems.push(format!(
                "{}: {} is wired for {} only and not marked `{marker}`",
                wiring.location,
                wiring.fixture,
                wiring.storage.title(),
            )),
            (None, Some(_)) => {}
            (Some(twin), Some(_)) => problems.push(format!(
                "{}: {} is marked `{marker}` but is also wired for {} at {}",
                wiring.location,
                wiring.fixture,
                other.title(),
                twin.location,
            )),
            (Some(twin), None) => {
                // Each pair is reported once, from its SQLite side
                if wiring.storage == Storage::Postgres {
                    continue;
                }
                for (setting, mine, theirs) in [
                    ("options", &wiring.options, &twin.options),
                    (
                        "extra_test_groups",
                        &wiring.extra_test_groups,
                        &twin.extra_test_groups,
                    ),
                ] {
                    if mine != theirs {
                        problems.push(format!(
                            "{}: {} has different `{setting}` for {} at {}",
                            wiring.location,
                            wiring.fixture,
                            other.title(),
                            twin.location,
                        ));
                    }
                }
            }
        }
    }

    problems
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../")
        .canonicalize()
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

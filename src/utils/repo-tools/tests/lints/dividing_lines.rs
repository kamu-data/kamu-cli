// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::fs::File;
use std::io;
use std::io::{Cursor, Write};
use std::path::PathBuf;

use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::Searcher;
use grep_searcher::sinks::UTF8;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn check_all_files_have_correct_dividing_lines() {
    let repo_root = get_repo_root();

    let mut bad_files = Vec::new();
    let matcher = build_incorrect_dividing_lines_matcher();

    for crate_path in get_all_crates() {
        let pattern = crate_path.join("**").join("*.rs");

        for entry in glob::glob(pattern.to_str().unwrap()).unwrap() {
            let file_path = entry.unwrap();
            let file_path_rel = file_path.strip_prefix(&repo_root).unwrap().to_owned();

            eprintln!("Checking file: {}", file_path_rel.display());

            let file = File::open(file_path).unwrap();
            let maybe_capture = get_incorrect_dividing_lines(&matcher, file_path_rel, file);

            if let Some(capture) = maybe_capture {
                bad_files.push(capture);
            }
        }
    }

    if !bad_files.is_empty() {
        let mut stderr = io::stderr().lock();

        writeln!(stderr).unwrap();
        writeln!(stderr, "Incorrect dividing lines are found:").unwrap();

        for capture in bad_files {
            for line in &capture.lines {
                // Make a clickable line for terminal
                writeln!(stderr, "./{}:{line}", capture.file_path_rel.display()).unwrap();
            }
        }

        writeln!(stderr).unwrap();

        panic!("Incorrect dividing lines are found in some files");
    }
}

#[test]
fn self_test() {
    let matcher = build_incorrect_dividing_lines_matcher();

    {
        let cursor = Cursor::new(indoc::indoc!(
            r#"
            // Licence header

            use kamu_accounts::{CreateAccessTokenError, RevokeTokenError};

            use crate::prelude::*;
            use crate::queries::{Account, CreateAccessTokenResultSuccess, CreatedAccessToken};
            use crate::utils::{check_access_token_valid, check_logged_account_id_match};

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            pub(crate) struct AuthMut;

            #[Object]
            impl AuthMut {
                async fn login(
                    &self,
                    ctx: &Context<'_>,
                    login_method: String,
                    login_credentials_json: String,
            "#
        ));

        assert_matches!(
            get_incorrect_dividing_lines(&matcher, PathBuf::from("./src/file.rs"), cursor),
            None
        );
    }

    {
        let cursor = Cursor::new(indoc::indoc!(
            r#"
            // Licence header

            use kamu_accounts::{CreateAccessTokenError, RevokeTokenError};

            use crate::utils::{check_access_token_valid, check_logged_account_id_match};

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            pub(crate) struct AuthMut;

            ///////////////////////////////////////////////////////////

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            "#
        ));

        assert_matches!(
            get_incorrect_dividing_lines(&matcher, PathBuf::from("./src/file.rs"), cursor),
            Some(CaptureIncorrectDividingLines {
                file_path_rel,
                lines
            }) if file_path_rel == PathBuf::from("./src/file.rs") && lines == vec![
                "11://///////////////////////////////////////////////////////// (len = 59)",
                "13://///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// (len = 123)"
            ]
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct CaptureIncorrectDividingLines {
    pub file_path_rel: PathBuf,
    pub lines: Vec<String>,
}

fn build_incorrect_dividing_lines_matcher() -> RegexMatcher {
    RegexMatcherBuilder::new()
        .whole_line(true)
        .build_many(&["/{4,119}", "/{121,}"])
        .unwrap()
}

fn get_incorrect_dividing_lines<T: io::Read>(
    matcher: &RegexMatcher,
    file_path_rel: PathBuf,
    readable: T,
) -> Option<CaptureIncorrectDividingLines> {
    let mut incorrect_dividing_lines = Vec::new();

    Searcher::new()
        .search_reader(
            matcher,
            readable,
            UTF8(|line_num, line| {
                let line = line.trim_end_matches('\n');
                let len = line.len();
                let num_with_line = format!("{line_num}:{line} (len = {len})");

                incorrect_dividing_lines.push(num_with_line);

                Ok(true)
            }),
        )
        .unwrap();

    if incorrect_dividing_lines.is_empty() {
        None
    } else {
        Some(CaptureIncorrectDividingLines {
            file_path_rel,
            lines: incorrect_dividing_lines,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Cargo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../")
        .canonicalize()
        .unwrap()
}

fn get_all_crates() -> Vec<PathBuf> {
    let repo_root = get_repo_root();

    let root_cargo_content = std::fs::read_to_string(repo_root.join("Cargo.toml"))
        .expect("Could not read root Cargo.toml file");

    let root_cargo: toml::Value = root_cargo_content
        .parse()
        .expect("Failed to parse root Cargo.toml");

    root_cargo["workspace"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| repo_root.join(v.as_str().unwrap()))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

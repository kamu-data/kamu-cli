// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Write};
use std::path::PathBuf;
use std::sync::LazyLock;

use pretty_assertions::assert_eq;
use regex::Regex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Production code that may write a dataset reference directly. Everything
/// else moves `HEAD` through `DatasetHeadCommitter`
const ALLOWED_REFERENCE_WRITES: &[AllowedReferenceWrite] = &[
    AllowedReferenceWrite {
        path: "src/domain/core/src/utils/metadata_chain_comparator.rs",
        method: "set_ref",
        reason: "wrapper that delegates to its inner chain",
    },
    AllowedReferenceWrite {
        path: "src/domain/datasets/services/src/services/odf/\
               metadata_chain_ref_repo_db_backed_impl.rs",
        method: "set_reference",
        reason: "database-backed chain references, reached only through the chain's set_ref",
    },
    AllowedReferenceWrite {
        path: "src/domain/datasets/services/src/services/odf/metadata_chain_db_backed/\
               metadata_chain_db_backed_impl.rs",
        method: "set_ref",
        reason: "delegates to its inner chain",
    },
    AllowedReferenceWrite {
        path: "src/domain/datasets/services/src/services/refs/dataset_head_committer_impl.rs",
        method: "set_ref",
        reason: "the HEAD commit primitive",
    },
    AllowedReferenceWrite {
        path: "src/domain/datasets/services/src/services/refs/dataset_reference_indexer.rs",
        method: "set_dataset_reference",
        reason: "startup mirror of storage references into the database",
    },
    AllowedReferenceWrite {
        path: "src/domain/datasets/services/src/services/refs/dataset_reference_service_impl.rs",
        method: "set_dataset_reference",
        reason: "the reference service writing its own repository",
    },
    AllowedReferenceWrite {
        path: "src/odf/dataset/",
        method: "set_ref",
        reason: "ODF layer: appending a batch to a chain whose HEAD is written directly",
    },
    AllowedReferenceWrite {
        path: "src/odf/dataset-impl/",
        method: "set_ref",
        reason: "ODF storage layer that implements references",
    },
];

const TEST_CODE_PATH_PARTS: &[&str] = &["/tests/", "/testing/", "/repo-tests/", "/benches/"];

const E2E_PATH_PREFIX: &str = "src/e2e/";

static TEST_MODULE_START: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(pub(\([^)]*\))?\s+)?mod\s").unwrap());

static REFERENCE_WRITE_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\.(set_ref|set_reference|set_dataset_reference)\s*(::<[^>]*>)?\(").unwrap()
});

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn check_reference_writes_go_through_head_committer() {
    let violations: Vec<_> = find_all_reference_writes()
        .into_iter()
        .filter(|write| find_allowance(write).is_none())
        .collect();

    if !violations.is_empty() {
        let mut stderr = io::stderr().lock();

        writeln!(stderr).unwrap();
        writeln!(stderr, "Direct dataset reference writes are found:").unwrap();
        for write in &violations {
            // Make a clickable line for terminal
            writeln!(
                stderr,
                "./{}:{}: {}",
                write.file_path_rel, write.line_num, write.code
            )
            .unwrap();
        }
        writeln!(stderr).unwrap();
        writeln!(
            stderr,
            "Move HEAD through `DatasetHeadCommitter::commit_head` instead, see \
             src/domain/datasets/domain/src/services/dataset_head_committer.rs. Exceptions are \
             listed in `ALLOWED_REFERENCE_WRITES` in \
             src/utils/repo-tools/tests/lints/reference_writes.rs"
        )
        .unwrap();

        panic!("Direct dataset reference writes are found in some files");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn check_allowed_reference_writes_are_used() {
    let writes = find_all_reference_writes();

    let unused: Vec<_> = ALLOWED_REFERENCE_WRITES
        .iter()
        .filter(|allowed| !writes.iter().any(|write| allowed.allows(write)))
        .map(|allowed| format!("{} ({}): {}", allowed.path, allowed.method, allowed.reason))
        .collect();

    assert!(
        unused.is_empty(),
        "Entries of `ALLOWED_REFERENCE_WRITES` match no call any more, remove them: {unused:#?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn self_test() {
    let production_code = indoc::indoc!(
        r#"
        // Licence header

        async fn commit(chain: &dyn MetadataChain) {
            // Mentioning set_ref() in a comment is fine
            chain.set_ref(&BlockRef::Head, &hash, opts).await.unwrap();

            chain
                .set_ref(&BlockRef::Head, &hash, opts)
                .await
                .unwrap();

            service.set_reference(&id, &BlockRef::Head, None, &hash).await.unwrap();
            repo.set_dataset_reference(&id, &BlockRef::Head, None, &hash).await.unwrap();
            let r = resolver.dataset_ref(&alias);
        }

        async fn set_ref(&self) {}

        #[cfg(test)]
        const TEST_ONLY: u8 = 0;

        fn after_test_only_item(chain: &dyn MetadataChain) {
            chain.set_ref(&BlockRef::Head, &hash, opts);
        }

        #[cfg(test)]
        mod tests {
            fn test_commit() {
                chain.set_ref(&BlockRef::Head, &hash, opts);
            }
        }
        "#
    );

    let violations = |path: &str| {
        find_reference_writes(path, Cursor::new(production_code))
            .into_iter()
            .filter(|write| find_allowance(write).is_none())
            .map(|write| format!("{}: {}", write.line_num, write.method))
            .collect::<Vec<_>>()
    };

    assert_eq!(
        violations("src/domain/foo/src/x.rs"),
        vec![
            "5: set_ref",
            "8: set_ref",
            "12: set_reference",
            "13: set_dataset_reference",
            "23: set_ref",
        ]
    );

    // The committer may call only `set_ref`
    assert_eq!(
        violations("src/domain/datasets/services/src/services/refs/dataset_head_committer_impl.rs"),
        vec!["12: set_reference", "13: set_dataset_reference"]
    );

    // The storage layer is allowed by directory
    assert_eq!(
        violations("src/odf/dataset-impl/src/entities/metadata_chain_impl.rs"),
        vec!["12: set_reference", "13: set_dataset_reference"]
    );

    // Test code is not checked
    assert_eq!(
        violations("src/domain/foo/tests/tests/test_x.rs"),
        Vec::<String>::new()
    );
    assert_eq!(
        violations("src/domain/foo/src/testing/x.rs"),
        Vec::<String>::new()
    );
    assert_eq!(
        violations("src/e2e/app/cli/repo-tests/src/x.rs"),
        Vec::<String>::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct ReferenceWrite {
    file_path_rel: String,
    line_num: usize,
    method: String,
    code: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AllowedReferenceWrite {
    /// A file, or a directory when it ends with `/`
    path: &'static str,
    method: &'static str,
    reason: &'static str,
}

impl AllowedReferenceWrite {
    fn allows(&self, write: &ReferenceWrite) -> bool {
        let path_matches = if self.path.ends_with('/') {
            write.file_path_rel.starts_with(self.path)
        } else {
            write.file_path_rel == self.path
        };

        path_matches && write.method == self.method
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn find_allowance(write: &ReferenceWrite) -> Option<&'static AllowedReferenceWrite> {
    ALLOWED_REFERENCE_WRITES
        .iter()
        .find(|allowed| allowed.allows(write))
}

fn is_test_code(file_path_rel: &str) -> bool {
    file_path_rel.starts_with(E2E_PATH_PREFIX)
        || TEST_CODE_PATH_PARTS
            .iter()
            .any(|part| file_path_rel.contains(part))
}

/// Reference write calls in production code, whether allowed or not
fn find_reference_writes<R: BufRead>(file_path_rel: &str, reader: R) -> Vec<ReferenceWrite> {
    if is_test_code(file_path_rel) {
        return Vec::new();
    }

    let mut writes = Vec::new();
    let mut after_cfg_test = false;

    for (index, line) in reader.lines().enumerate() {
        let line = line.unwrap();
        let code = line.trim();

        if code.starts_with("//") {
            continue;
        }
        if code.starts_with("#[cfg(test)]") {
            after_cfg_test = true;
            continue;
        }
        if after_cfg_test {
            // A test module is the tail of the file; any other test-only item
            // ends where production code resumes
            if TEST_MODULE_START.is_match(code) {
                break;
            }
            if code.is_empty() || code.starts_with("#[") {
                continue;
            }
            after_cfg_test = false;
        }

        for captures in REFERENCE_WRITE_CALL.captures_iter(code) {
            writes.push(ReferenceWrite {
                file_path_rel: file_path_rel.to_string(),
                line_num: index + 1,
                method: captures[1].to_string(),
                code: code.to_string(),
            });
        }
    }

    writes
}

fn find_all_reference_writes() -> Vec<ReferenceWrite> {
    let repo_root = get_repo_root();

    let file_paths: BTreeSet<PathBuf> = get_all_crates()
        .into_iter()
        .flat_map(|crate_path| {
            let pattern = crate_path.join("**").join("*.rs");
            glob::glob(pattern.to_str().unwrap())
                .unwrap()
                .map(Result::unwrap)
        })
        .collect();

    file_paths
        .into_iter()
        .flat_map(|file_path| {
            let file_path_rel = file_path
                .strip_prefix(&repo_root)
                .unwrap()
                .to_str()
                .unwrap()
                .replace('\\', "/");
            let file = BufReader::new(File::open(&file_path).unwrap());

            find_reference_writes(&file_path_rel, file)
        })
        .collect()
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

    let root_cargo: toml::Value =
        toml::from_str(&root_cargo_content).expect("Failed to parse root Cargo.toml");

    root_cargo["workspace"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| repo_root.join(v.as_str().unwrap()))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

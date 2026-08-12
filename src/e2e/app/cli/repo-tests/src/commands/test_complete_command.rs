// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_subcommand(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu l", 1).await;

    assert_eq!(completions, ["list", "log", "login", "logout"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_config(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu config set engine.runt", 3).await;

    assert_eq!(completions, ["engine.runtime"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_dataset_name(kamu: KamuCliPuppet) {
    kamu.add_dataset(
        odf::DatasetSnapshot {
            name: "foo.bar".try_into().unwrap(),
            kind: odf::DatasetKind::Root,
            metadata: vec![],
        },
        AddDatasetOptions::default(),
    )
    .await;

    let completions = kamu.complete("kamu log", 2).await;

    assert_eq!(completions, ["foo.bar"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_resource_type_for_get(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu get", 2).await;

    assert!(completions.contains(&"variablesets".to_string()));
    assert!(completions.contains(&"vs".to_string()));
    assert!(completions.contains(&"secretsets".to_string()));
    assert!(completions.contains(&"ss".to_string()));
    assert!(completions.contains(&"storages".to_string()));
    assert!(completions.contains(&"st".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_resource_type_for_list(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list", 2).await;

    assert!(completions.contains(&"datasets".to_string()));
    assert!(completions.contains(&"all".to_string()));
    assert!(completions.contains(&"variablesets".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_resource_type_for_delete(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu delete", 2).await;

    assert!(completions.contains(&"datasets".to_string()));
    assert!(completions.contains(&"all".to_string()));
    assert!(completions.contains(&"secretsets".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_resource_type_only_for_first_get_token(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu get vs my-vars", 3).await;

    assert!(completions.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_context_name(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu context", 2).await;

    assert!(completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `--as` takes a new dataset name, not a context name.
pub async fn test_complete_pull_as_does_not_suggest_context_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu pull foo.bar --as", 4).await;

    assert!(!completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A preceding option must not be miscounted as a positional, which would
// otherwise make the completer think it's past the first `get` token.
pub async fn test_complete_resource_type_for_get_after_flag(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu get --spec", 3).await;

    assert!(completions.contains(&"variablesets".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The second `delete` token is a name/pattern, not another type selector.
pub async fn test_complete_resource_type_only_for_first_delete_token(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu delete variablesets d", 3).await;

    assert!(completions.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// An explicit `--context` that doesn't resolve to a known context must not
// silently fall back to completing against the local/active context.
pub async fn test_complete_resource_type_honors_explicit_context(kamu: KamuCliPuppet) {
    let completions = kamu
        .complete("kamu list --context nonexistent-context", 4)
        .await;

    assert_eq!(completions, ["datasets", "all"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `context add` takes a *new* name, so it should not suggest existing ones.
pub async fn test_complete_context_add_does_not_suggest_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu context add", 3).await;

    assert!(!completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `context use`/`check`/`delete` all take an *existing* context name.
pub async fn test_complete_context_use_suggests_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu context use", 3).await;

    assert!(completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `rename`'s second positional is a new dataset name, not a context name.
pub async fn test_complete_rename_new_name_does_not_suggest_context_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu rename foo.bar", 3).await;

    assert!(!completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `repo add`'s first positional is a new repository alias, not a context name.
pub async fn test_complete_repo_add_name_does_not_suggest_context_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu repo add", 3).await;

    assert!(!completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `New.dataset_name` is a positional new dataset name, not a context name.
pub async fn test_complete_new_dataset_does_not_suggest_context_names(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu new", 2).await;

    assert!(!completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A leading global option must not stop subcommand discovery: `get`'s
// resource-type completion should still trigger after `-v`.
pub async fn test_complete_resource_type_for_get_after_global_flag(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu -v get", 3).await;

    assert!(completions.contains(&"variablesets".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `--context=value` (attached form) must resolve the same as `--context value`.
pub async fn test_complete_resource_type_honors_attached_explicit_context(kamu: KamuCliPuppet) {
    let completions = kamu
        .complete("kamu list --context=nonexistent-context", 3)
        .await;

    assert_eq!(completions, ["datasets", "all"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `-c` (short form) must complete context names just like `--context`.
pub async fn test_complete_context_name_for_short_flag(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list -c", 3).await;

    assert!(completions.contains(&"local".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A cursor index beyond the parsed token count (e.g. stale/mismatched
// COMP_CWORD) must not panic; it should simply yield no completions.
pub async fn test_complete_out_of_range_cursor_does_not_panic(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu get", 99).await;

    assert!(completions.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A clustered short option (`-wc prod` == `-w -c prod`) must still resolve
// the `-c` value; a bogus context falls back to only the static targets.
pub async fn test_complete_resource_type_honors_clustered_short_context(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list -wc nonexistent-context", 4).await;

    assert_eq!(completions, ["datasets", "all"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `-c` completing as the current token (not yet a finished preceding token)
// must emit full `-cNAME` candidates, not bare names.
pub async fn test_complete_context_name_for_attached_short_flag(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list -c", 2).await;

    assert!(completions.contains(&"-clocal".to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Bash makes `=` the current word at `--context=<TAB>`, then makes the value
// fragment current once a prefix is typed. Both need bare-name candidates.
pub async fn test_complete_context_name_for_split_long_flag(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list --context =", 3).await;

    assert!(completions.contains(&"local".to_string()));

    let completions = kamu.complete("kamu list --context = lo", 4).await;

    assert_eq!(completions, ["local"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kamu_cli::WorkspaceLayout;
use kamu_cli::config::{CLIConfig, ConfigService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_config_defaults() {
    let cfg = ConfigService::new(&WorkspaceLayout::new("."))
        .load_from(&[])
        .unwrap();
    pretty_assertions::assert_eq!(cfg, CLIConfig::default());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_config_database_provider_tag() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cfg_path = tmp_dir.path().join("1.yaml");

    std::fs::write(
        &cfg_path,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                database:
                    provider: sqlite
                    databasePath: .kamu/db
            "#
        ),
    )
    .unwrap();

    let cfg = ConfigService::new(&WorkspaceLayout::new("."))
        .load_from(&[cfg_path])
        .unwrap();

    pretty_assertions::assert_eq!(
        cfg,
        CLIConfig {
            database: Some(kamu_cli::config::DatabaseConfig::Sqlite(
                kamu_cli::config::SqliteDatabaseConfig {
                    database_path: ".kamu/db".into()
                }
            )),
            ..Default::default()
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_config_merging_simple() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cfg_path_1 = tmp_dir.path().join("1.yaml");
    let cfg_path_2 = tmp_dir.path().join("2.yaml");

    std::fs::write(
        &cfg_path_1,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                engine:
                    maxConcurrency: 3
            "#
        ),
    )
    .unwrap();

    std::fs::write(
        &cfg_path_2,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                engine:
                    runtime: podman
            "#
        ),
    )
    .unwrap();

    let cfg = ConfigService::new(&WorkspaceLayout::new("."))
        .load_from(&[cfg_path_1, cfg_path_2])
        .unwrap();

    pretty_assertions::assert_eq!(
        cfg,
        CLIConfig {
            engine: kamu_cli::config::EngineConfig {
                max_concurrency: Some(3),
                runtime: container_runtime::ContainerRuntimeType::Podman,
                ..Default::default()
            },
            ..Default::default()
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_config_merging_of_evm_chains() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cfg_path_1 = tmp_dir.path().join("1.yaml");
    let cfg_path_2 = tmp_dir.path().join("2.yaml");

    std::fs::write(
        &cfg_path_1,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                source:
                    ethereum:
                        rpcEndpoints:
                            - chainId: 1
                              chainName: Ethereum Mainnet
                              nodeUrl: wss://mainnet.eth/
            "#
        ),
    )
    .unwrap();

    std::fs::write(
        &cfg_path_2,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                source:
                    ethereum:
                        rpcEndpoints:
                            - chainId: 11155111
                              chainName: Ethereum Sepolia Testnet
                              nodeUrl: wss://sepolia.eth/
            "#
        ),
    )
    .unwrap();

    let cfg = ConfigService::new(&WorkspaceLayout::new("."))
        .load_from(&[cfg_path_1, cfg_path_2])
        .unwrap();

    pretty_assertions::assert_eq!(
        cfg,
        CLIConfig {
            source: kamu_cli::config::SourceConfig {
                ethereum: kamu_cli::config::EthereumSourceConfig {
                    rpc_endpoints: vec![
                        kamu_cli::config::EthRpcEndpoint {
                            chain_id: 1,
                            chain_name: "Ethereum Mainnet".into(),
                            node_url: "wss://mainnet.eth/".parse().unwrap(),
                        },
                        kamu_cli::config::EthRpcEndpoint {
                            chain_id: 11_155_111,
                            chain_name: "Ethereum Sepolia Testnet".into(),
                            node_url: "wss://sepolia.eth/".parse().unwrap(),
                        }
                    ],
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_config_merging_of_engine_options() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cfg_path_1 = tmp_dir.path().join("1.yaml");
    let cfg_path_2 = tmp_dir.path().join("2.yaml");

    std::fs::write(
        &cfg_path_1,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                engine:
                    datafusionEmbedded:
                        base:
                            'datafusion.catalog.information_schema': 'false'
            "#
        ),
    )
    .unwrap();

    std::fs::write(
        &cfg_path_2,
        indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
                engine:
                    datafusionEmbedded:
                        ingest:
                            'something.else': 'blah'
            "#
        ),
    )
    .unwrap();

    let cfg = ConfigService::new(&WorkspaceLayout::new("."))
        .load_from(&[cfg_path_1, cfg_path_2])
        .unwrap();

    pretty_assertions::assert_eq!(
        cfg,
        CLIConfig {
            engine: kamu_cli::config::EngineConfig {
                datafusion_embedded: kamu_cli::config::EngineConfigDatafusion {
                    base: BTreeMap::from([
                        (
                            "datafusion.catalog.default_catalog".to_string(),
                            "kamu".to_string()
                        ),
                        (
                            "datafusion.catalog.default_schema".to_string(),
                            "kamu".to_string()
                        ),
                        (
                            "datafusion.catalog.information_schema".to_string(),
                            "false".to_string()
                        ),
                        (
                            "datafusion.sql_parser.enable_ident_normalization".to_string(),
                            "false".to_string()
                        ),
                    ]),
                    ingest: BTreeMap::from([
                        (
                            "datafusion.execution.target_partitions".to_string(),
                            "1".to_string()
                        ),
                        ("something.else".to_string(), "blah".to_string()),
                    ]),
                    batch_query: BTreeMap::from([]),
                    compaction: BTreeMap::from([(
                        "datafusion.execution.target_partitions".to_string(),
                        "1".to_string()
                    ),]),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(resourcegen)]
#[test]
fn update_config_schema() {
    let fig = setty::Config::<CLIConfig>::new();
    let schema = fig.json_schema().to_string_pretty();

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../../resources/config-schema.json");

    std::fs::write(&path, schema).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(resourcegen)]
#[test]
fn update_config_markdown() {
    let fig = setty::Config::<CLIConfig>::new();
    let md = fig.markdown();

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../../resources/config-reference.md");

    std::fs::write(&path, md).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

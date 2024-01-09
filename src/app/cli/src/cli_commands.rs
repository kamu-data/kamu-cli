// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::CurrentAccountSubject;
use opendatafabric::*;
use url::Url;

use crate::commands::*;
use crate::config::UsersConfig;
use crate::{accounts, odf_server, CommandInterpretationFailed, WorkspaceService};

pub fn get_command(
    base_catalog: &dill::Catalog,
    cli_catalog: &dill::Catalog,
    arg_matches: clap::ArgMatches,
) -> Result<Box<dyn Command>, CLIError> {
    let command: Box<dyn Command> = match arg_matches.subcommand() {
        Some(("add", submatches)) => {
            let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
            let user_config = cli_catalog.get_one::<UsersConfig>()?;

            Box::new(AddCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                accounts::AccountService::current_account_indication(
                    &arg_matches,
                    workspace_svc.is_multi_tenant_workspace(),
                    user_config.as_ref(),
                ),
                submatches
                    .get_many("manifest")
                    .unwrap_or_default()
                    .map(String::as_str),
                submatches.get_flag("recursive"),
                submatches.get_flag("replace"),
                submatches.get_flag("stdin"),
            ))
        }
        Some(("complete", submatches)) => {
            let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
            let in_workspace =
                workspace_svc.is_in_workspace() && !workspace_svc.is_upgrade_needed()?;

            Box::new(CompleteCommand::new(
                if in_workspace {
                    Some(cli_catalog.get_one()?)
                } else {
                    None
                },
                if in_workspace {
                    Some(cli_catalog.get_one()?)
                } else {
                    None
                },
                if in_workspace {
                    Some(cli_catalog.get_one()?)
                } else {
                    None
                },
                cli_catalog.get_one()?,
                crate::cli_parser::cli(),
                submatches
                    .get_one::<String>("input")
                    .map(|s| s.to_string())
                    .unwrap(),
                *(submatches.get_one("current").unwrap()),
            ))
        }
        Some(("completions", submatches)) => Box::new(CompletionsCommand::new(
            crate::cli_parser::cli(),
            *submatches.get_one("shell").unwrap(),
        )),
        Some(("config", config_matches)) => match config_matches.subcommand() {
            Some(("list", list_matches)) => Box::new(ConfigListCommand::new(
                cli_catalog.get_one()?,
                list_matches.get_flag("user"),
                list_matches.get_flag("with-defaults"),
            )),
            Some(("get", get_matches)) => Box::new(ConfigGetCommand::new(
                cli_catalog.get_one()?,
                get_matches.get_flag("user"),
                get_matches.get_flag("with-defaults"),
                get_matches.get_one::<String>("cfgkey").unwrap().to_string(),
            )),
            Some(("set", set_matches)) => Box::new(ConfigSetCommand::new(
                cli_catalog.get_one()?,
                set_matches.get_flag("user"),
                set_matches.get_one::<String>("cfgkey").unwrap().to_string(),
                set_matches.get_one("value").map(|s: &String| s.to_owned()),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("delete", submatches)) => Box::new(DeleteCommand::new(
            cli_catalog.get_one()?,
            validate_many_dataset_refs(
                cli_catalog,
                submatches
                    .get_many("dataset")
                    .unwrap() // required
                    .map(|r: &DatasetRef| r.clone()),
            )?,
            submatches.get_flag("all"),
            submatches.get_flag("recursive"),
            submatches.get_flag("yes"),
        )),
        Some(("ingest", submatches)) => Box::new(IngestCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(
                cli_catalog,
                submatches.get_one::<DatasetRef>("dataset").unwrap().clone(),
            )?,
            submatches
                .get_many("file")
                .unwrap_or_default()
                .map(String::as_str),
            submatches.get_one("source-name").map(String::as_str),
            submatches.get_flag("stdin"),
            submatches.get_flag("recursive"),
            submatches.get_one("input-format").map(String::as_str),
        )),
        Some(("init", submatches)) => {
            if submatches.get_flag("pull-images") {
                Box::new(PullImagesCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    submatches.get_flag("list-only"),
                ))
            } else {
                Box::new(InitCommand::new(
                    cli_catalog.get_one()?,
                    submatches.get_flag("multi-tenant"),
                ))
            }
        }
        Some(("inspect", submatches)) => match submatches.subcommand() {
            Some(("lineage", lin_matches)) => Box::new(LineageCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                validate_many_dataset_refs(
                    cli_catalog,
                    lin_matches
                        .get_many("dataset")
                        .unwrap() // required
                        .map(|r: &DatasetRef| r.clone()),
                )?,
                lin_matches.get_flag("browse"),
                lin_matches.get_one("output-format").map(String::as_str),
                cli_catalog.get_one()?,
            )),
            Some(("query", query_matches)) => Box::new(InspectQueryCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                validate_dataset_ref(
                    cli_catalog,
                    query_matches
                        .get_one::<DatasetRef>("dataset")
                        .unwrap()
                        .clone(),
                )?,
                cli_catalog.get_one()?,
            )),
            Some(("schema", schema_matches)) => Box::new(InspectSchemaCommand::new(
                cli_catalog.get_one()?,
                validate_dataset_ref(
                    cli_catalog,
                    schema_matches
                        .get_one::<DatasetRef>("dataset")
                        .unwrap()
                        .clone(),
                )?,
                schema_matches.get_one("output-format").map(String::as_str),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("list", submatches)) => {
            let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
            let user_config = cli_catalog.get_one::<UsersConfig>()?;

            Box::new(ListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                accounts::AccountService::current_account_indication(
                    &arg_matches,
                    workspace_svc.is_multi_tenant_workspace(),
                    user_config.as_ref(),
                ),
                accounts::AccountService::related_account_indication(submatches),
                cli_catalog.get_one()?,
                submatches.get_count("wide"),
            ))
        }
        Some(("log", submatches)) => Box::new(LogCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(
                cli_catalog,
                submatches.get_one::<DatasetRef>("dataset").unwrap().clone(),
            )?,
            submatches.get_one("output-format").map(String::as_str),
            submatches.get_one("filter").map(String::as_str),
            *(submatches.get_one("limit").unwrap()),
            cli_catalog.get_one()?,
        )),
        Some(("login", submatches)) => Box::new(LoginCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            if submatches.get_flag("user") {
                odf_server::AccessTokenStoreScope::User
            } else {
                odf_server::AccessTokenStoreScope::Workspace
            },
            // TODO: improve URL parser
            submatches
                .get_one::<String>("server")
                .map(|s| Url::parse(s).unwrap()),
        )),
        Some(("logout", submatches)) => Box::new(LogoutCommand::new(
            cli_catalog.get_one()?,
            if submatches.get_flag("user") {
                odf_server::AccessTokenStoreScope::User
            } else {
                odf_server::AccessTokenStoreScope::Workspace
            },
            // TODO: improve URL parser
            submatches
                .get_one::<String>("server")
                .map(|s| Url::parse(s).unwrap()),
        )),
        Some(("new", submatches)) => Box::new(NewDatasetCommand::new(
            submatches.get_one::<DatasetName>("name").unwrap().clone(),
            submatches.get_flag("root"),
            submatches.get_flag("derivative"),
            None::<&str>,
        )),
        Some(("notebook", submatches)) => Box::new(NotebookCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            submatches.get_one("address").map(|a| *a),
            submatches.get_one("http-port").map(|p| *p),
            submatches
                .get_many("env")
                .unwrap_or_default() // optional
                .map(String::as_str),
        )),
        Some(("pull", submatches)) => {
            let datasets = submatches
                .get_many("dataset")
                .unwrap_or_default() // optional
                .map(|r: &DatasetRefAny| r.clone());
            if submatches.contains_id("set-watermark") {
                if datasets.len() != 1 {}
                Box::new(SetWatermarkCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    datasets,
                    submatches.get_flag("all"),
                    submatches.get_flag("recursive"),
                    submatches
                        .get_one("set-watermark")
                        .map(String::as_str)
                        .unwrap(),
                ))
            } else {
                Box::new(PullCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    datasets,
                    submatches.get_flag("all"),
                    submatches.get_flag("recursive"),
                    submatches.get_flag("fetch-uncacheable"),
                    submatches.get_one("as").map(|s: &DatasetName| s.clone()),
                    !submatches.get_flag("no-alias"),
                    submatches.get_flag("force"),
                ))
            }
        }
        Some(("push", push_matches)) => Box::new(PushCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            push_matches
                .get_many("dataset")
                .unwrap_or_default()
                .map(|r: &DatasetRefAny| r.clone()),
            push_matches.get_flag("all"),
            push_matches.get_flag("recursive"),
            !push_matches.get_flag("no-alias"),
            push_matches.get_flag("force"),
            push_matches
                .get_one("to")
                .map(|t: &DatasetRefRemote| t.clone()),
            cli_catalog.get_one()?,
        )),
        Some(("rename", rename_matches)) => Box::new(RenameCommand::new(
            cli_catalog.get_one()?,
            validate_dataset_ref(
                cli_catalog,
                rename_matches
                    .get_one::<DatasetRef>("dataset")
                    .unwrap()
                    .clone(),
            )?,
            rename_matches.get_one("name").map(String::as_str).unwrap(),
        )),
        Some(("repo", repo_matches)) => match repo_matches.subcommand() {
            Some(("add", add_matches)) => Box::new(RepositoryAddCommand::new(
                cli_catalog.get_one()?,
                add_matches.get_one::<RepoName>("name").unwrap().clone(),
                add_matches.get_one("url").map(String::as_str).unwrap(),
            )),
            Some(("delete", delete_matches)) => Box::new(RepositoryDeleteCommand::new(
                cli_catalog.get_one()?,
                delete_matches
                    .get_many("repository")
                    .unwrap_or_default() // optional
                    .map(|rn: &RepoName| rn.clone()),
                delete_matches.get_flag("all"),
                delete_matches.get_flag("yes"),
            )),
            Some(("list", _)) => Box::new(RepositoryListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
            )),
            Some(("alias", alias_matches)) => match alias_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(AliasAddCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    add_matches
                        .get_one::<DatasetRef>("dataset")
                        .unwrap()
                        .clone(),
                    add_matches
                        .get_one::<DatasetRefRemote>("alias")
                        .unwrap()
                        .clone(),
                    add_matches.get_flag("pull"),
                    add_matches.get_flag("push"),
                )),
                Some(("delete", delete_matches)) => Box::new(AliasDeleteCommand::new(
                    cli_catalog.get_one()?,
                    delete_matches
                        .get_one::<DatasetRef>("dataset")
                        .unwrap()
                        .clone(),
                    delete_matches
                        .get_one("alias")
                        .map(|a: &DatasetRefRemote| a.clone()),
                    delete_matches.get_flag("all"),
                    delete_matches.get_flag("pull"),
                    delete_matches.get_flag("push"),
                )),
                Some(("list", list_matches)) => Box::new(AliasListCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    list_matches
                        .get_one("dataset")
                        .map(|s: &DatasetRef| s.clone()),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("reset", submatches)) => Box::new(ResetCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(
                cli_catalog,
                submatches.get_one::<DatasetRef>("dataset").unwrap().clone(),
            )?,
            submatches.get_one::<Multihash>("hash").unwrap().clone(),
            submatches.get_flag("yes"),
        )),
        Some(("search", submatches)) => Box::new(SearchCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            submatches.get_one("query").map(String::as_str),
            submatches
                .get_many("repo")
                .unwrap_or_default() // optional
                .map(|rn: &RepoName| rn.clone()),
        )),
        Some(("sql", submatches)) => match submatches.subcommand() {
            None => Box::new(SqlShellCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                submatches.get_one("command").map(String::as_str),
                submatches.get_one("url").map(String::as_str),
                submatches.get_one("engine").map(String::as_str),
            )),
            Some(("server", server_matches)) => {
                if server_matches.get_flag("livy") {
                    Box::new(SqlServerLivyCommand::new(
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        *server_matches.get_one("address").unwrap(),
                        *(server_matches.get_one("port").unwrap()),
                    ))
                } else if server_matches.get_flag("flight-sql") {
                    Box::new(SqlServerFlightSqlCommand::new(
                        *server_matches.get_one("address").unwrap(),
                        *(server_matches.get_one("port").unwrap()),
                        cli_catalog.get_one()?,
                    ))
                } else {
                    Box::new(SqlServerCommand::new(
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        *server_matches.get_one("address").unwrap(),
                        *(server_matches.get_one("port").unwrap()),
                    ))
                }
            }
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("system", submatches)) => match submatches.subcommand() {
            Some(("gc", _)) => Box::new(GcCommand::new(cli_catalog.get_one()?)),
            Some(("upgrade-workspace", _)) => {
                Box::new(UpgradeWorkspaceCommand::new(cli_catalog.get_one()?))
            }
            Some(("api-server", server_matches)) => match server_matches.subcommand() {
                None => {
                    let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;

                    Box::new(APIServerRunCommand::new(
                        base_catalog.clone(),
                        workspace_svc.is_multi_tenant_workspace(),
                        cli_catalog.get_one()?,
                        server_matches.get_one("address").map(|a| *a),
                        server_matches.get_one("http-port").map(|p| *p),
                        server_matches.get_flag("get-token"),
                        cli_catalog.get_one()?,
                    ))
                }
                Some(("gql-query", query_matches)) => Box::new(APIServerGqlQueryCommand::new(
                    base_catalog.clone(),
                    query_matches.get_one("query").map(String::as_str).unwrap(),
                    query_matches.get_flag("full"),
                )),
                Some(("gql-schema", _)) => Box::new(APIServerGqlSchemaCommand {}),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            Some(("info", info_matches)) => Box::new(SystemInfoCommand::new(
                cli_catalog.get_one()?,
                info_matches.get_one("output-format").map(String::as_str),
            )),
            Some(("diagnose", _)) => Box::new(SystemDiagnoseCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
            )),
            Some(("ipfs", ipfs_matches)) => match ipfs_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(SystemIpfsAddCommand::new(
                    cli_catalog.get_one()?,
                    add_matches
                        .get_one::<DatasetRef>("dataset")
                        .unwrap()
                        .clone(),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("tail", submatches)) => Box::new(TailCommand::new(
            cli_catalog.get_one()?,
            validate_dataset_ref(
                cli_catalog,
                submatches.get_one::<DatasetRef>("dataset").unwrap().clone(),
            )?,
            *(submatches.get_one("skip-records").unwrap()),
            *(submatches.get_one("num-records").unwrap()),
            cli_catalog.get_one()?,
        )),
        Some(("ui", submatches)) => {
            let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;

            let current_account_subject = cli_catalog.get_one::<CurrentAccountSubject>()?;

            let current_account_name = match current_account_subject.as_ref() {
                CurrentAccountSubject::Logged(l) => l.account_name.clone(),
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Cannot launch Web UI with anonymous account")
                }
            };

            Box::new(UICommand::new(
                base_catalog.clone(),
                workspace_svc.is_multi_tenant_workspace(),
                current_account_name,
                cli_catalog.get_one()?,
                submatches.get_one("address").map(|a| *a),
                submatches.get_one("http-port").map(|p| *p),
                submatches.get_flag("get-token"),
            ))
        }
        Some(("verify", submatches)) => Box::new(VerifyCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_many_dataset_refs(
                cli_catalog,
                submatches
                    .get_many("dataset")
                    .unwrap() // required
                    .map(|r: &DatasetRef| r.clone()),
            )?
            .into_iter(),
            submatches.get_flag("recursive"),
            submatches.get_flag("integrity"),
        )),
        Some(("version", submatches)) => Box::new(VersionCommand::new(
            cli_catalog.get_one()?,
            submatches.get_one("output-format").map(String::as_str),
        )),
        _ => return Err(CommandInterpretationFailed.into()),
    };

    Ok(command)
}

fn validate_dataset_ref(
    catalog: &dill::Catalog,
    dataset_ref: DatasetRef,
) -> Result<DatasetRef, CLIError> {
    if let DatasetRef::Alias(ref alias) = dataset_ref {
        let workspace_svc = catalog.get_one::<WorkspaceService>()?;
        if !workspace_svc.is_multi_tenant_workspace() && alias.is_multi_tenant() {
            return Err(MultiTenantRefUnexpectedError {
                dataset_ref: dataset_ref.clone(),
            }
            .into());
        }
    }
    return Ok(dataset_ref);
}

fn validate_many_dataset_refs<I>(
    catalog: &dill::Catalog,
    dataset_refs: I,
) -> Result<Vec<DatasetRef>, CLIError>
where
    I: IntoIterator<Item = DatasetRef>,
{
    let mut result_refs = Vec::new();
    for dataset_ref in dataset_refs.into_iter() {
        result_refs.push(validate_dataset_ref(catalog, dataset_ref)?);
    }

    return Ok(result_refs);
}

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetName;
use opendatafabric::DatasetRefAny;
use opendatafabric::DatasetRefLocal;
use opendatafabric::DatasetRefRemote;
use opendatafabric::Multihash;
use opendatafabric::RepositoryName;

use crate::app::in_workspace;
use crate::commands::*;
use crate::CommandInterpretationFailed;

pub fn get_command(
    catalog: &dill::Catalog,
    matches: clap::ArgMatches,
) -> Result<Box<dyn Command>, CLIError> {
    let command: Box<dyn Command> = match matches.subcommand() {
        Some(("add", submatches)) => Box::new(AddCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.get_many("manifest").unwrap().map(String::as_str), // required
            submatches.get_flag("recursive"),
            submatches.get_flag("replace"),
        )),
        Some(("complete", submatches)) => Box::new(CompleteCommand::new(
            if in_workspace(catalog.get_one()?) {
                Some(catalog.get_one()?)
            } else {
                None
            },
            if in_workspace(catalog.get_one()?) {
                Some(catalog.get_one()?)
            } else {
                None
            },
            if in_workspace(catalog.get_one()?) {
                Some(catalog.get_one()?)
            } else {
                None
            },
            catalog.get_one()?,
            crate::cli_parser::cli(),
            submatches
                .get_one::<String>("input")
                .map(|s| s.to_string())
                .unwrap(),
            *(submatches.get_one("current").unwrap()),
        )),
        Some(("completions", submatches)) => Box::new(CompletionsCommand::new(
            crate::cli_parser::cli(),
            *submatches.get_one("shell").unwrap(),
        )),
        Some(("config", config_matches)) => match config_matches.subcommand() {
            Some(("list", list_matches)) => Box::new(ConfigListCommand::new(
                catalog.get_one()?,
                list_matches.get_flag("user"),
                list_matches.get_flag("with-defaults"),
            )),
            Some(("get", get_matches)) => Box::new(ConfigGetCommand::new(
                catalog.get_one()?,
                get_matches.get_flag("user"),
                get_matches.get_flag("with-defaults"),
                get_matches.get_one::<String>("cfgkey").unwrap().to_string(),
            )),
            Some(("set", set_matches)) => Box::new(ConfigSetCommand::new(
                catalog.get_one()?,
                set_matches.get_flag("user"),
                set_matches.get_one::<String>("cfgkey").unwrap().to_string(),
                set_matches.get_one("value").map(|s: &String| s.to_owned()),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("delete", submatches)) => Box::new(DeleteCommand::new(
            catalog.get_one()?,
            submatches
                .get_many("dataset")
                .unwrap() // required
                .map(|r: &DatasetRefLocal| r.clone()),
            submatches.get_flag("all"),
            submatches.get_flag("recursive"),
            submatches.get_flag("yes"),
        )),
        Some(("init", submatches)) => {
            if submatches.get_flag("pull-images") || submatches.get_flag("pull-test-images") {
                Box::new(PullImagesCommand::new(
                    catalog.get_one()?,
                    submatches.get_flag("pull-test-images"),
                    submatches.get_flag("list-only"),
                ))
            } else {
                Box::new(InitCommand::new(catalog.get_one()?))
            }
        }
        Some(("inspect", submatches)) => match submatches.subcommand() {
            Some(("lineage", lin_matches)) => Box::new(LineageCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                lin_matches
                    .get_many("dataset")
                    .unwrap() // required
                    .map(|r: &DatasetRefLocal| r.clone()),
                lin_matches.get_flag("browse"),
                lin_matches.get_one("output-format").map(String::as_str),
                catalog.get_one()?,
            )),
            Some(("query", query_matches)) => Box::new(InspectQueryCommand::new(
                catalog.get_one()?,
                query_matches
                    .get_one::<DatasetRefLocal>("dataset")
                    .unwrap()
                    .clone(),
                catalog.get_one()?,
            )),
            Some(("schema", schema_matches)) => Box::new(InspectSchemaCommand::new(
                catalog.get_one()?,
                schema_matches
                    .get_one::<DatasetRefLocal>("dataset")
                    .unwrap()
                    .clone(),
                schema_matches.get_one("output-format").map(String::as_str),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("list", submatches)) => Box::new(ListCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.get_count("wide"),
        )),
        Some(("log", submatches)) => Box::new(LogCommand::new(
            catalog.get_one()?,
            submatches
                .get_one::<DatasetRefLocal>("dataset")
                .unwrap()
                .clone(),
            submatches.get_one("output-format").map(String::as_str),
            submatches.get_one("filter").map(String::as_str),
            *(submatches.get_one("limit").unwrap()),
            catalog.get_one()?,
        )),
        Some(("new", submatches)) => Box::new(NewDatasetCommand::new(
            submatches.get_one::<DatasetName>("name").unwrap().clone(),
            submatches.get_flag("root"),
            submatches.get_flag("derivative"),
            None::<&str>,
        )),
        Some(("notebook", submatches)) => Box::new(NotebookCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
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
                    catalog.get_one()?,
                    catalog.get_one()?,
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
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    datasets,
                    submatches.get_flag("all"),
                    submatches.get_flag("recursive"),
                    submatches.get_flag("fetch-uncacheable"),
                    submatches.get_one("as").map(|s: &DatasetName| s.clone()),
                    !submatches.get_flag("no-alias"),
                    submatches.get_one("fetch").map(String::as_str),
                    submatches.get_flag("force"),
                ))
            }
        }
        Some(("push", push_matches)) => Box::new(PushCommand::new(
            catalog.get_one()?,
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
            catalog.get_one()?,
        )),
        Some(("rename", rename_matches)) => Box::new(RenameCommand::new(
            catalog.get_one()?,
            rename_matches
                .get_one::<DatasetRefLocal>("dataset")
                .unwrap()
                .clone(),
            rename_matches.get_one("name").map(String::as_str).unwrap(),
        )),
        Some(("repo", repo_matches)) => match repo_matches.subcommand() {
            Some(("add", add_matches)) => Box::new(RepositoryAddCommand::new(
                catalog.get_one()?,
                add_matches
                    .get_one::<RepositoryName>("name")
                    .unwrap()
                    .clone(),
                add_matches.get_one("url").map(String::as_str).unwrap(),
            )),
            Some(("delete", delete_matches)) => Box::new(RepositoryDeleteCommand::new(
                catalog.get_one()?,
                delete_matches
                    .get_many("repository")
                    .unwrap_or_default() // optional
                    .map(|rn: &RepositoryName| rn.clone()),
                delete_matches.get_flag("all"),
                delete_matches.get_flag("yes"),
            )),
            Some(("list", _)) => Box::new(RepositoryListCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
            )),
            Some(("alias", alias_matches)) => match alias_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(AliasAddCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    add_matches
                        .get_one::<DatasetRefLocal>("dataset")
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
                    catalog.get_one()?,
                    delete_matches
                        .get_one::<DatasetRefLocal>("dataset")
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
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    list_matches
                        .get_one("dataset")
                        .map(|s: &DatasetRefLocal| s.clone()),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("reset", submatches)) => Box::new(ResetCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches
                .get_one::<DatasetRefLocal>("dataset")
                .unwrap()
                .clone(),
            submatches.get_one::<Multihash>("hash").unwrap().clone(),
            submatches.get_flag("yes"),
        )),
        Some(("search", submatches)) => Box::new(SearchCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.get_one("query").map(String::as_str),
            submatches
                .get_many("repo")
                .unwrap_or_default() // optional
                .map(|rn: &RepositoryName| rn.clone()),
        )),
        Some(("sql", submatches)) => match submatches.subcommand() {
            None => Box::new(SqlShellCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                submatches.get_one("command").map(String::as_str),
                submatches.get_one("url").map(String::as_str),
                submatches.get_one("engine").map(String::as_str),
            )),
            Some(("server", server_matches)) => {
                if !server_matches.get_flag("livy") {
                    Box::new(SqlServerCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        *(server_matches.get_one("address").unwrap()),
                        *(server_matches.get_one("port").unwrap()),
                    ))
                } else {
                    Box::new(SqlServerLivyCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        server_matches
                            .get_one("address")
                            .map(String::as_str)
                            .unwrap(),
                        *(server_matches.get_one("port").unwrap()),
                    ))
                }
            }
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("system", submatches)) => match submatches.subcommand() {
            Some(("api-server", server_matches)) => match server_matches.subcommand() {
                None => Box::new(APIServerRunCommand::new(
                    catalog.clone(), // TODO: Currently very expensive!
                    catalog.get_one()?,
                    server_matches.get_one("address").map(|a| *a),
                    server_matches.get_one("http-port").map(|p| *p),
                )),
                Some(("gql-query", query_matches)) => Box::new(APIServerGqlQueryCommand::new(
                    catalog.clone(), // TODO: Currently very expensive!
                    query_matches.get_one("query").map(String::as_str).unwrap(),
                    query_matches.get_flag("full"),
                )),
                Some(("gql-schema", _)) => Box::new(APIServerGqlSchemaCommand::new(
                    catalog.clone(), // TODO: Currently very expensive
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            Some(("ipfs", ipfs_matches)) => match ipfs_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(SystemIpfsAddCommand::new(
                    catalog.get_one()?,
                    add_matches
                        .get_one::<DatasetRefLocal>("dataset")
                        .unwrap()
                        .clone(),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("tail", submatches)) => Box::new(TailCommand::new(
            catalog.get_one()?,
            submatches
                .get_one::<DatasetRefLocal>("dataset")
                .unwrap()
                .clone(),
            *(submatches.get_one("num-records").unwrap()),
            catalog.get_one()?,
        )),
        Some(("ui", submatches)) => Box::new(UICommand::new(
            catalog.clone(), // TODO: Currently very expensive!
            catalog.get_one()?,
            submatches.get_one("address").map(|a| *a),
            submatches.get_one("http-port").map(|p| *p),
        )),
        Some(("verify", submatches)) => Box::new(VerifyCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches
                .get_many("dataset")
                .unwrap() // required
                .map(|r: &DatasetRefLocal| r.clone()),
            submatches.get_flag("recursive"),
            submatches.get_flag("integrity"),
        )),
        _ => return Err(CommandInterpretationFailed.into()),
    };

    Ok(command)
}

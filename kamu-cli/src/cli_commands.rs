// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
            submatches.values_of("manifest").unwrap(),
            submatches.is_present("recursive"),
            submatches.is_present("replace"),
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
            submatches.value_of("input").unwrap().into(),
            submatches.value_of("current").unwrap().parse().unwrap(),
        )),
        Some(("completions", submatches)) => Box::new(CompletionsCommand::new(
            crate::cli_parser::cli(),
            submatches.value_of_t_or_exit("shell"),
        )),
        Some(("config", config_matches)) => match config_matches.subcommand() {
            Some(("list", list_matches)) => Box::new(ConfigListCommand::new(
                catalog.get_one()?,
                list_matches.is_present("user"),
                list_matches.is_present("with-defaults"),
            )),
            Some(("get", get_matches)) => Box::new(ConfigGetCommand::new(
                catalog.get_one()?,
                get_matches.is_present("user"),
                get_matches.is_present("with-defaults"),
                get_matches.value_of("cfgkey").unwrap().to_owned(),
            )),
            Some(("set", set_matches)) => Box::new(ConfigSetCommand::new(
                catalog.get_one()?,
                set_matches.is_present("user"),
                set_matches.value_of("cfgkey").unwrap().to_owned(),
                set_matches.value_of("value").map(|s| s.to_owned()),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("delete", submatches)) => Box::new(DeleteCommand::new(
            catalog.get_one()?,
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            submatches.is_present("yes"),
        )),
        Some(("init", submatches)) => {
            if submatches.is_present("pull-images") || submatches.is_present("pull-test-images") {
                Box::new(PullImagesCommand::new(
                    catalog.get_one()?,
                    submatches.is_present("pull-test-images"),
                    submatches.is_present("list-only"),
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
                lin_matches.values_of("dataset").unwrap_or_default(),
                lin_matches.is_present("browse"),
                lin_matches.value_of("output-format"),
                catalog.get_one()?,
            )),
            Some(("query", query_matches)) => Box::new(InspectQueryCommand::new(
                catalog.get_one()?,
                query_matches.value_of_t_or_exit("dataset"),
                catalog.get_one()?,
            )),
            Some(("schema", schema_matches)) => Box::new(InspectSchemaCommand::new(
                catalog.get_one()?,
                schema_matches.value_of_t_or_exit("dataset"),
                schema_matches.value_of("output-format"),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("list", submatches)) => Box::new(ListCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.occurrences_of("wide") as u8,
        )),
        Some(("log", submatches)) => Box::new(LogCommand::new(
            catalog.get_one()?,
            submatches.value_of_t_or_exit("dataset"),
            submatches.value_of("output-format"),
            submatches.value_of("filter"),
            catalog.get_one()?,
        )),
        Some(("new", submatches)) => Box::new(NewDatasetCommand::new(
            submatches.value_of("name").unwrap(),
            submatches.is_present("root"),
            submatches.is_present("derivative"),
            None::<&str>,
        )),
        Some(("notebook", submatches)) => Box::new(NotebookCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.values_of("env").unwrap_or_default(),
        )),
        Some(("pull", submatches)) => {
            if submatches.is_present("set-watermark") {
                let datasets = submatches.values_of("dataset").unwrap_or_default();
                if datasets.len() != 1 {}
                Box::new(SetWatermarkCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.value_of("set-watermark").unwrap(),
                ))
            } else {
                Box::new(PullCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.is_present("fetch-uncacheable"),
                    submatches.value_of("as"),
                    !submatches.is_present("no-alias"),
                    submatches.value_of("fetch"),
                ))
            }
        }
        Some(("push", push_matches)) => Box::new(PushCommand::new(
            catalog.get_one()?,
            push_matches.values_of("dataset").unwrap_or_default(),
            push_matches.is_present("all"),
            push_matches.is_present("recursive"),
            !push_matches.is_present("no-alias"),
            push_matches.value_of("to"),
            catalog.get_one()?,
        )),
        Some(("rename", rename_matches)) => Box::new(RenameCommand::new(
            catalog.get_one()?,
            rename_matches.value_of("dataset").unwrap(),
            rename_matches.value_of("name").unwrap(),
        )),
        Some(("repo", repo_matches)) => match repo_matches.subcommand() {
            Some(("add", add_matches)) => Box::new(RepositoryAddCommand::new(
                catalog.get_one()?,
                add_matches.value_of("name").unwrap(),
                add_matches.value_of("url").unwrap(),
            )),
            Some(("delete", delete_matches)) => Box::new(RepositoryDeleteCommand::new(
                catalog.get_one()?,
                delete_matches.values_of("repository").unwrap_or_default(),
                delete_matches.is_present("all"),
                delete_matches.is_present("yes"),
            )),
            Some(("list", _)) => Box::new(RepositoryListCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
            )),
            Some(("alias", alias_matches)) => match alias_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(AliasAddCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    add_matches.value_of("dataset").unwrap(),
                    add_matches.value_of("alias").unwrap(),
                    add_matches.is_present("pull"),
                    add_matches.is_present("push"),
                )),
                Some(("delete", delete_matches)) => Box::new(AliasDeleteCommand::new(
                    catalog.get_one()?,
                    delete_matches.value_of("dataset").unwrap(),
                    delete_matches.value_of("alias"),
                    delete_matches.is_present("all"),
                    delete_matches.is_present("pull"),
                    delete_matches.is_present("push"),
                )),
                Some(("list", list_matches)) => Box::new(AliasListCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    catalog.get_one()?,
                    list_matches.value_of("dataset"),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("search", submatches)) => Box::new(SearchCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.value_of("query"),
            submatches.values_of("repo").unwrap_or_default(),
        )),
        Some(("sql", submatches)) => match submatches.subcommand() {
            None => Box::new(SqlShellCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                submatches.value_of("command"),
                submatches.value_of("url"),
                submatches.value_of("engine"),
            )),
            Some(("server", server_matches)) => {
                if !server_matches.is_present("livy") {
                    Box::new(SqlServerCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        server_matches.value_of("address").unwrap(),
                        server_matches.value_of_t_or_exit("port"),
                    ))
                } else {
                    Box::new(SqlServerLivyCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        server_matches.value_of("address").unwrap(),
                        server_matches.value_of_t_or_exit("port"),
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
                    server_matches.value_of_t("address").ok(),
                    server_matches.value_of_t("http-port").ok(),
                )),
                Some(("gql-query", query_matches)) => Box::new(APIServerGqlQueryCommand::new(
                    catalog.clone(), // TODO: Currently very expensive!
                    query_matches.value_of("query").unwrap(),
                    query_matches.is_present("full"),
                )),
                Some(("gql-schema", _)) => Box::new(APIServerGqlSchemaCommand::new(
                    catalog.clone(), // TODO: Currently very expensive
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            Some(("ipfs", ipfs_matches)) => match ipfs_matches.subcommand() {
                Some(("add", add_matches)) => Box::new(SystemIpfsAddCommand::new(
                    catalog.get_one()?,
                    add_matches.value_of("dataset").unwrap(),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        Some(("tail", submatches)) => Box::new(TailCommand::new(
            catalog.get_one()?,
            submatches.value_of("dataset").unwrap(),
            submatches.value_of_t_or_exit("num-records"),
            catalog.get_one()?,
        )),
        Some(("ui", submatches)) => Box::new(UICommand::new(
            catalog.clone(), // TODO: Currently very expensive!
            catalog.get_one()?,
            submatches.value_of_t("address").ok(),
            submatches.value_of_t("http-port").ok(),
        )),
        Some(("verify", submatches)) => Box::new(VerifyCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("recursive"),
            submatches.is_present("integrity"),
        )),
        _ => return Err(CommandInterpretationFailed.into()),
    };

    Ok(command)
}

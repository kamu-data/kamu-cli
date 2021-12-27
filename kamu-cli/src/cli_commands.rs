// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::value_t_or_exit;
use opendatafabric::*;

use crate::app::in_workspace;
use crate::commands::*;
use crate::CommandInterpretationFailed;

pub fn get_command(
    catalog: &dill::Catalog,
    matches: clap::ArgMatches,
) -> Result<Box<dyn Command>, CLIError> {
    let command: Box<dyn Command> = match matches.subcommand() {
        ("add", Some(submatches)) => Box::new(AddCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.values_of("manifest").unwrap(),
            submatches.is_present("recursive"),
            submatches.is_present("replace"),
        )),
        ("complete", Some(submatches)) => Box::new(CompleteCommand::new(
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
        ("completions", Some(submatches)) => Box::new(CompletionsCommand::new(
            crate::cli_parser::cli(),
            value_t_or_exit!(submatches.value_of("shell"), clap::Shell),
        )),
        ("config", Some(config_matches)) => match config_matches.subcommand() {
            ("list", Some(list_matches)) => Box::new(ConfigListCommand::new(
                catalog.get_one()?,
                list_matches.is_present("user"),
                list_matches.is_present("with-defaults"),
            )),
            ("get", Some(get_matches)) => Box::new(ConfigGetCommand::new(
                catalog.get_one()?,
                get_matches.is_present("user"),
                get_matches.is_present("with-defaults"),
                get_matches.value_of("cfgkey").unwrap().to_owned(),
            )),
            ("set", Some(set_matches)) => Box::new(ConfigSetCommand::new(
                catalog.get_one()?,
                set_matches.is_present("user"),
                set_matches.value_of("cfgkey").unwrap().to_owned(),
                set_matches.value_of("value").map(|s| s.to_owned()),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        ("delete", Some(submatches)) => Box::new(DeleteCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            submatches.is_present("yes"),
        )),
        ("init", Some(submatches)) => {
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
        ("inspect", Some(submatches)) => match submatches.subcommand() {
            ("lineage", Some(lin_matches)) => Box::new(LineageCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                lin_matches.values_of("dataset").unwrap_or_default(),
                lin_matches.is_present("browse"),
                lin_matches.value_of("output-format"),
                catalog.get_one()?,
            )),
            ("query", Some(query_matches)) => Box::new(InspectQueryCommand::new(
                catalog.get_one()?,
                value_t_or_exit!(query_matches.value_of("dataset"), DatasetRefLocal),
                catalog.get_one()?,
            )),
            ("schema", Some(schema_matches)) => Box::new(InspectSchemaCommand::new(
                catalog.get_one()?,
                value_t_or_exit!(schema_matches.value_of("dataset"), DatasetRefLocal),
                schema_matches.value_of("output-format"),
            )),
            _ => return Err(CommandInterpretationFailed.into()),
        },
        ("list", Some(submatches)) => Box::new(ListCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.occurrences_of("wide") as u8,
        )),
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            catalog.get_one()?,
            value_t_or_exit!(submatches.value_of("dataset"), DatasetRefLocal),
            submatches.value_of("output-format"),
            submatches.value_of("filter"),
            catalog.get_one()?,
        )),
        ("new", Some(submatches)) => Box::new(NewDatasetCommand::new(
            value_t_or_exit!(submatches.value_of("name"), DatasetName),
            submatches.is_present("root"),
            submatches.is_present("derivative"),
            None::<&str>,
        )),
        ("notebook", Some(submatches)) => Box::new(NotebookCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.values_of("env").unwrap_or_default(),
        )),
        ("pull", Some(submatches)) => {
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
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.is_present("force-uncacheable"),
                    submatches.value_of("as"),
                    submatches.value_of("fetch"),
                ))
            }
        }
        ("push", Some(push_matches)) => Box::new(PushCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            push_matches.values_of("dataset").unwrap_or_default(),
            push_matches.is_present("all"),
            push_matches.is_present("recursive"),
            push_matches.value_of("as"),
            catalog.get_one()?,
        )),
        ("repo", Some(repo_matches)) => match repo_matches.subcommand() {
            ("add", Some(add_matches)) => Box::new(RepositoryAddCommand::new(
                catalog.get_one()?,
                add_matches.value_of("name").unwrap(),
                add_matches.value_of("url").unwrap(),
            )),
            ("delete", Some(delete_matches)) => Box::new(RepositoryDeleteCommand::new(
                catalog.get_one()?,
                delete_matches.values_of("repository").unwrap_or_default(),
                delete_matches.is_present("all"),
                delete_matches.is_present("yes"),
            )),
            ("list", _) => Box::new(RepositoryListCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
            )),
            ("alias", Some(alias_matches)) => match alias_matches.subcommand() {
                ("add", Some(add_matches)) => Box::new(AliasAddCommand::new(
                    catalog.get_one()?,
                    add_matches.value_of("dataset").unwrap(),
                    add_matches.value_of("alias").unwrap(),
                    add_matches.is_present("pull"),
                    add_matches.is_present("push"),
                )),
                ("delete", Some(delete_matches)) => Box::new(AliasDeleteCommand::new(
                    catalog.get_one()?,
                    delete_matches.value_of("dataset").unwrap(),
                    delete_matches.value_of("alias"),
                    delete_matches.is_present("all"),
                    delete_matches.is_present("pull"),
                    delete_matches.is_present("push"),
                )),
                ("list", Some(list_matches)) => Box::new(AliasListCommand::new(
                    catalog.get_one()?,
                    catalog.get_one()?,
                    list_matches.value_of("dataset"),
                )),
                _ => return Err(CommandInterpretationFailed.into()),
            },
            _ => return Err(CommandInterpretationFailed.into()),
        },
        ("search", Some(submatches)) => Box::new(SearchCommand::new(
            catalog.get_one()?,
            catalog.get_one()?,
            submatches.value_of("query"),
            submatches.values_of("repo").unwrap_or_default(),
        )),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new(
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                catalog.get_one()?,
                submatches.value_of("command"),
                submatches.value_of("url"),
                submatches.value_of("engine"),
            )),
            ("server", Some(server_matches)) => {
                if !server_matches.is_present("livy") {
                    Box::new(SqlServerCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        server_matches.value_of("address").unwrap(),
                        value_t_or_exit!(server_matches.value_of("port"), u16),
                    ))
                } else {
                    Box::new(SqlServerLivyCommand::new(
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        catalog.get_one()?,
                        server_matches.value_of("address").unwrap(),
                        value_t_or_exit!(server_matches.value_of("port"), u16),
                    ))
                }
            }
            _ => return Err(CommandInterpretationFailed.into()),
        },
        ("tail", Some(submatches)) => Box::new(TailCommand::new(
            catalog.get_one()?,
            value_t_or_exit!(submatches.value_of("dataset"), DatasetRefLocal),
            value_t_or_exit!(submatches.value_of("num-records"), u64),
            catalog.get_one()?,
        )),
        ("verify", Some(submatches)) => Box::new(VerifyCommand::new(
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

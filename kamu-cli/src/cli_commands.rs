use clap::value_t_or_exit;
use opendatafabric::DatasetIDBuf;
use slog::Logger;

use crate::app::in_workspace;
use crate::commands::*;

pub fn get_command(catalog: &dill::Catalog, matches: clap::ArgMatches) -> Box<dyn Command> {
    match matches.subcommand() {
        ("add", Some(submatches)) => Box::new(AddCommand::new(
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            submatches.values_of("manifest").unwrap(),
            submatches.is_present("recursive"),
            submatches.is_present("replace"),
        )),
        ("complete", Some(submatches)) => Box::new(CompleteCommand::new(
            if in_workspace(catalog.get_one().unwrap()) {
                Some(catalog.get_one().unwrap())
            } else {
                None
            },
            catalog.get_one().unwrap(),
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
                catalog.get_one().unwrap(),
                list_matches.is_present("user"),
                list_matches.is_present("with-defaults"),
            )),
            ("get", Some(get_matches)) => Box::new(ConfigGetCommand::new(
                catalog.get_one().unwrap(),
                get_matches.is_present("user"),
                get_matches.is_present("with-defaults"),
                get_matches.value_of("cfgkey").unwrap().to_owned(),
            )),
            ("set", Some(set_matches)) => Box::new(ConfigSetCommand::new(
                catalog.get_one().unwrap(),
                set_matches.is_present("user"),
                set_matches.value_of("cfgkey").unwrap().to_owned(),
                set_matches.value_of("value").map(|s| s.to_owned()),
            )),
            _ => unimplemented!(),
        },
        ("delete", Some(submatches)) => Box::new(DeleteCommand::new(
            catalog.get_one().unwrap(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            submatches.is_present("yes"),
        )),
        ("init", Some(submatches)) => {
            if submatches.is_present("pull-images") {
                Box::new(PullImagesCommand::new(catalog.get_one().unwrap(), false))
            } else if submatches.is_present("pull-test-images") {
                Box::new(PullImagesCommand::new(catalog.get_one().unwrap(), true))
            } else {
                Box::new(InitCommand::new(catalog.get_one().unwrap()))
            }
        }
        ("list", Some(submatches)) => match submatches.subcommand() {
            ("", _) => Box::new(ListCommand::new(
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
            )),
            ("depgraph", _) => Box::new(DepgraphCommand::new(catalog.get_one().unwrap())),
            _ => unimplemented!(),
        },
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            catalog.get_one().unwrap(),
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
            catalog.get_one().unwrap(),
        )),
        ("new", Some(submatches)) => Box::new(NewDatasetCommand::new(
            submatches.value_of("id").unwrap(),
            submatches.is_present("root"),
            submatches.is_present("derivative"),
            None::<&str>,
        )),
        ("notebook", Some(submatches)) => Box::new(NotebookCommand::new(
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            submatches.values_of("env").unwrap_or_default(),
            catalog.get_one::<Logger>().unwrap().as_ref().clone(),
        )),
        ("pull", Some(submatches)) => {
            if submatches.is_present("set-watermark") {
                let datasets = submatches.values_of("dataset").unwrap_or_default();
                if datasets.len() != 1 {}
                Box::new(SetWatermarkCommand::new(
                    catalog.get_one().unwrap(),
                    catalog.get_one().unwrap(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.value_of("set-watermark").unwrap(),
                ))
            } else {
                Box::new(PullCommand::new(
                    catalog.get_one().unwrap(),
                    catalog.get_one().unwrap(),
                    catalog.get_one().unwrap(),
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
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            push_matches.values_of("dataset").unwrap_or_default(),
            push_matches.is_present("all"),
            push_matches.is_present("recursive"),
            push_matches.value_of("as"),
            catalog.get_one().unwrap(),
        )),
        ("repo", Some(repo_matches)) => match repo_matches.subcommand() {
            ("add", Some(add_matches)) => Box::new(RepositoryAddCommand::new(
                catalog.get_one().unwrap(),
                add_matches.value_of("name").unwrap(),
                add_matches.value_of("url").unwrap(),
            )),
            ("delete", Some(delete_matches)) => Box::new(RepositoryDeleteCommand::new(
                catalog.get_one().unwrap(),
                delete_matches.values_of("repo").unwrap_or_default(),
                delete_matches.is_present("all"),
                delete_matches.is_present("yes"),
            )),
            ("list", _) => Box::new(RepositoryListCommand::new(
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
            )),
            ("alias", Some(alias_matches)) => match alias_matches.subcommand() {
                ("add", Some(add_matches)) => Box::new(AliasAddCommand::new(
                    catalog.get_one().unwrap(),
                    add_matches.value_of("dataset").unwrap().to_owned(),
                    add_matches.value_of("alias").unwrap().to_owned(),
                    add_matches.is_present("pull"),
                    add_matches.is_present("push"),
                )),
                ("delete", Some(delete_matches)) => Box::new(AliasDeleteCommand::new(
                    catalog.get_one().unwrap(),
                    delete_matches.value_of("dataset").unwrap().to_owned(),
                    delete_matches.value_of("alias").map(|s| s.to_owned()),
                    delete_matches.is_present("all"),
                    delete_matches.is_present("pull"),
                    delete_matches.is_present("push"),
                )),
                ("list", Some(list_matches)) => Box::new(AliasListCommand::new(
                    catalog.get_one().unwrap(),
                    catalog.get_one().unwrap(),
                    list_matches.value_of("dataset").map(|s| s.to_owned()),
                )),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        },
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new(
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
                submatches.value_of("command"),
                submatches.value_of("url"),
                catalog.get_one::<Logger>().unwrap().as_ref().clone(),
            )),
            ("server", Some(server_matches)) => {
                if !server_matches.is_present("livy") {
                    Box::new(SqlServerCommand::new(
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one::<Logger>().unwrap().as_ref().clone(),
                        server_matches.value_of("address").unwrap(),
                        value_t_or_exit!(server_matches.value_of("port"), u16),
                    ))
                } else {
                    Box::new(SqlServerLivyCommand::new(
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one().unwrap(),
                        catalog.get_one::<Logger>().unwrap().as_ref().clone(),
                        server_matches.value_of("address").unwrap(),
                        value_t_or_exit!(server_matches.value_of("port"), u16),
                    ))
                }
            }
            _ => unimplemented!(),
        },
        ("verify", Some(submatches)) => Box::new(VerifyCommand::new(
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("recursive"),
        )),
        _ => unimplemented!(),
    }
}

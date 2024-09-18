// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::CommandFactory as _;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;

use crate::commands::*;
use crate::{accounts, cli, odf_server, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_command(
    base_catalog: &dill::Catalog,
    cli_catalog: &dill::Catalog,
    args: cli::Cli,
) -> Result<Box<dyn Command>, CLIError> {
    let command: Box<dyn Command> = match args.command {
        cli::Command::Add(c) => Box::new(AddCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.manifest,
            c.name,
            c.recursive,
            c.replace,
            c.stdin,
            c.visibility.into(),
            cli_catalog.get_one()?,
        )),
        cli::Command::Complete(c) => {
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
                crate::cli::Cli::command(),
                c.input,
                c.current,
            ))
        }
        cli::Command::Completions(c) => {
            Box::new(CompletionsCommand::new(crate::cli::Cli::command(), c.shell))
        }
        cli::Command::Config(c) => match c.subcommand {
            cli::ConfigSubCommand::List(sc) => Box::new(ConfigListCommand::new(
                cli_catalog.get_one()?,
                sc.user,
                sc.with_defaults,
            )),
            cli::ConfigSubCommand::Get(sc) => Box::new(ConfigGetCommand::new(
                cli_catalog.get_one()?,
                sc.user,
                sc.with_defaults,
                sc.cfgkey,
            )),
            cli::ConfigSubCommand::Set(sc) => Box::new(ConfigSetCommand::new(
                cli_catalog.get_one()?,
                sc.user,
                sc.cfgkey,
                sc.value,
            )),
        },
        cli::Command::Delete(c) => Box::new(DeleteCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_many_dataset_patterns(cli_catalog, c.dataset)?,
            cli_catalog.get_one()?,
            c.all,
            c.recursive,
            c.yes,
        )),
        cli::Command::Ingest(c) => Box::new(IngestCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.file.unwrap_or_default(),
            c.source_name,
            c.event_time,
            c.stdin,
            c.recursive,
            c.input_format,
        )),
        cli::Command::Init(c) => {
            if c.pull_images {
                Box::new(PullImagesCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    c.list_only,
                ))
            } else {
                Box::new(InitCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    c.exists_ok,
                    c.multi_tenant,
                ))
            }
        }
        cli::Command::Inspect(c) => match c.subcommand {
            cli::InspectSubCommand::Lineage(sc) => Box::new(LineageCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                validate_many_dataset_refs(cli_catalog, sc.dataset)?,
                sc.browse,
                sc.output_format,
                cli_catalog.get_one()?,
            )),
            cli::InspectSubCommand::Query(sc) => Box::new(InspectQueryCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                validate_dataset_ref(cli_catalog, sc.dataset)?,
                cli_catalog.get_one()?,
            )),
            cli::InspectSubCommand::Schema(sc) => Box::new(InspectSchemaCommand::new(
                cli_catalog.get_one()?,
                validate_dataset_ref(cli_catalog, sc.dataset)?,
                sc.output_format,
                sc.from_data_file,
            )),
        },
        cli::Command::List(c) => {
            let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
            let user_config = cli_catalog.get_one::<kamu_accounts::PredefinedAccountsConfig>()?;

            Box::new(ListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                accounts::AccountService::current_account_indication(
                    args.account,
                    workspace_svc.is_multi_tenant_workspace(),
                    user_config.as_ref(),
                ),
                accounts::AccountService::related_account_indication(
                    c.target_account,
                    c.all_accounts,
                ),
                cli_catalog.get_one()?,
                c.wide,
            ))
        }
        cli::Command::Log(c) => Box::new(LogCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.output_format,
            c.filter,
            c.limit,
            cli_catalog.get_one()?,
        )),
        cli::Command::Login(c) => match c.subcommand {
            Some(cli::LoginSubCommand::Oauth(sc)) => Box::new(LoginSilentCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                if sc.user {
                    odf_server::AccessTokenStoreScope::User
                } else {
                    odf_server::AccessTokenStoreScope::Workspace
                },
                sc.server.map(Into::into),
                LoginSilentMode::OAuth(LoginSilentModeOAuth {
                    provider: sc.provider,
                    access_token: sc.access_token,
                }),
            )),
            Some(cli::LoginSubCommand::Password(sc)) => Box::new(LoginSilentCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                if sc.user {
                    odf_server::AccessTokenStoreScope::User
                } else {
                    odf_server::AccessTokenStoreScope::Workspace
                },
                sc.server.map(Into::into),
                LoginSilentMode::Password(LoginSilentModePassword {
                    login: sc.login,
                    password: sc.password,
                }),
            )),
            None => Box::new(LoginCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                if c.user {
                    odf_server::AccessTokenStoreScope::User
                } else {
                    odf_server::AccessTokenStoreScope::Workspace
                },
                c.server.map(Into::into),
                c.access_token,
                c.check,
            )),
        },
        cli::Command::Logout(c) => Box::new(LogoutCommand::new(
            cli_catalog.get_one()?,
            if c.user {
                odf_server::AccessTokenStoreScope::User
            } else {
                odf_server::AccessTokenStoreScope::Workspace
            },
            c.server.map(Into::into),
            c.all,
        )),
        cli::Command::New(c) => Box::new(NewDatasetCommand::new(
            c.name,
            c.root,
            c.derivative,
            None::<&str>,
        )),
        cli::Command::Notebook(c) => Box::new(NotebookCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.address,
            c.http_port,
            c.env.unwrap_or_default(),
        )),
        cli::Command::Pull(c) => {
            if let Some(set_watermark) = c.set_watermark {
                Box::new(SetWatermarkCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    c.dataset.unwrap_or_default(),
                    c.all,
                    c.recursive,
                    set_watermark,
                ))
            } else {
                Box::new(PullCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    c.dataset.unwrap_or_default(),
                    cli_catalog.get_one()?,
                    c.all,
                    c.recursive,
                    c.fetch_uncacheable,
                    c.r#as,
                    !c.no_alias,
                    c.force,
                    c.reset_derivatives_on_diverged_input,
                ))
            }
        }
        cli::Command::Push(c) => Box::new(PushCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.dataset.unwrap_or_default(),
            cli_catalog.get_one()?,
            c.all,
            c.recursive,
            !c.no_alias,
            c.force,
            c.to,
            c.visibility.into(),
            cli_catalog.get_one()?,
        )),
        cli::Command::Rename(c) => Box::new(RenameCommand::new(
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.name,
        )),
        cli::Command::Repo(c) => match c.subcommand {
            cli::RepoSubCommand::Add(sc) => Box::new(RepositoryAddCommand::new(
                cli_catalog.get_one()?,
                sc.name,
                sc.url,
            )),
            cli::RepoSubCommand::Delete(sc) => Box::new(RepositoryDeleteCommand::new(
                cli_catalog.get_one()?,
                sc.repository.unwrap_or_default(),
                sc.all,
                sc.yes,
            )),
            cli::RepoSubCommand::List(_) => Box::new(RepositoryListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
            )),
            cli::RepoSubCommand::Alias(sc) => match sc.subcommand {
                cli::RepoAliasSubCommand::Add(ssc) => Box::new(AliasAddCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    ssc.dataset,
                    ssc.alias,
                    ssc.pull,
                    ssc.push,
                )),
                cli::RepoAliasSubCommand::Delete(ssc) => Box::new(AliasDeleteCommand::new(
                    cli_catalog.get_one()?,
                    ssc.dataset,
                    ssc.alias,
                    ssc.all,
                    ssc.pull,
                    ssc.push,
                )),
                cli::RepoAliasSubCommand::List(ssc) => Box::new(AliasListCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    ssc.dataset,
                )),
            },
        },
        cli::Command::Reset(c) => Box::new(ResetCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.hash,
            c.yes,
        )),
        cli::Command::Search(c) => Box::new(SearchCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.query,
            c.repo.unwrap_or_default(),
        )),
        cli::Command::Sql(c) => match c.subcommand {
            None => Box::new(SqlShellCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                c.command,
                c.url,
                c.engine,
            )),
            Some(cli::SqlSubCommand::Server(sc)) => {
                if sc.livy {
                    Box::new(SqlServerLivyCommand::new(
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        sc.address,
                        sc.port,
                    ))
                } else if sc.flight_sql {
                    cfg_if::cfg_if! {
                        if #[cfg(feature = "flight-sql")] {
                            Box::new(SqlServerFlightSqlCommand::new(
                                sc.address,
                                sc.port,
                                cli_catalog.get_one()?,
                            ))
                        } else {
                            return Err(CLIError::usage_error("Kamu was compiled without Flight SQL support"))
                        }
                    }
                } else {
                    Box::new(SqlServerCommand::new(
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        sc.address,
                        sc.port,
                    ))
                }
            }
        },
        cli::Command::System(c) => match c.subcommand {
            cli::SystemSubCommand::ApiServer(sc) => match sc.subcommand {
                None => {
                    let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;

                    Box::new(APIServerRunCommand::new(
                        base_catalog.clone(),
                        cli_catalog.clone(),
                        workspace_svc.is_multi_tenant_workspace(),
                        cli_catalog.get_one()?,
                        sc.address,
                        sc.http_port,
                        sc.external_address,
                        sc.get_token,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        cli_catalog.get_one()?,
                        args.e2e_output_data_path,
                    ))
                }
                Some(cli::SystemApiServerSubCommand::GqlQuery(ssc)) => Box::new(
                    APIServerGqlQueryCommand::new(base_catalog.clone(), ssc.query, ssc.full),
                ),
                Some(cli::SystemApiServerSubCommand::GqlSchema(_)) => {
                    Box::new(APIServerGqlSchemaCommand {})
                }
            },
            cli::SystemSubCommand::Compact(sc) => Box::new(CompactCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                validate_dataset_ref(cli_catalog, sc.dataset)?,
                sc.max_slice_size,
                sc.max_slice_records,
                sc.hard,
                sc.verify,
                sc.keep_metadata_only,
            )),
            cli::SystemSubCommand::Diagnose(_) => Box::new(SystemDiagnoseCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
            )),
            cli::SystemSubCommand::DebugToken(sc) => {
                Box::new(DebugTokenCommand::new(cli_catalog.get_one()?, sc.token))
            }
            cli::SystemSubCommand::E2e(sc) => Box::new(SystemE2ECommand::new(
                sc.action,
                sc.dataset,
                cli_catalog.get_one()?,
            )),
            cli::SystemSubCommand::Gc(_) => Box::new(GcCommand::new(cli_catalog.get_one()?)),
            cli::SystemSubCommand::GenerateToken(sc) => Box::new(GenerateTokenCommand::new(
                cli_catalog.get_one()?,
                sc.login,
                sc.subject,
                sc.expiration_time_sec,
            )),
            cli::SystemSubCommand::Info(sc) => Box::new(SystemInfoCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                sc.output_format,
            )),
            cli::SystemSubCommand::Ipfs(sc) => match sc.subcommand {
                cli::SystemIpfsSubCommand::Add(ssc) => Box::new(SystemIpfsAddCommand::new(
                    cli_catalog.get_one()?,
                    ssc.dataset,
                )),
            },
            cli::SystemSubCommand::UpgradeWorkspace(_) => {
                Box::new(UpgradeWorkspaceCommand::new(cli_catalog.get_one()?))
            }
        },
        cli::Command::Tail(c) => Box::new(TailCommand::new(
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.skip_records,
            c.num_records,
            cli_catalog.get_one()?,
        )),
        cli::Command::Ui(c) => {
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
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                c.address,
                c.http_port,
                c.get_token,
            ))
        }
        cli::Command::Verify(c) => Box::new(VerifyCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            validate_many_dataset_patterns(cli_catalog, c.dataset)?.into_iter(),
            c.recursive,
            c.integrity,
        )),

        cli::Command::Version(c) => {
            Box::new(VersionCommand::new(cli_catalog.get_one()?, c.output_format))
        }
    };

    Ok(command)
}

#[allow(clippy::match_like_matches_macro)]
pub fn command_needs_transaction(args: &cli::Cli) -> bool {
    match &args.command {
        cli::Command::System(c) => match &c.subcommand {
            cli::SystemSubCommand::GenerateToken(_) => true,
            _ => false,
        },
        cli::Command::Delete(_) => true,
        _ => false,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Dataset reference validation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_dataset_ref(
    catalog: &dill::Catalog,
    dataset_ref: DatasetRef,
) -> Result<DatasetRef, CLIError> {
    if let DatasetRef::Alias(ref alias) = dataset_ref {
        let workspace_svc = catalog.get_one::<WorkspaceService>()?;
        if !workspace_svc.is_multi_tenant_workspace() && alias.is_multi_tenant() {
            return Err(MultiTenantRefUnexpectedError {
                dataset_ref_pattern: DatasetRefPattern::Ref(dataset_ref),
            }
            .into());
        }
    }
    Ok(dataset_ref)
}

fn validate_dataset_ref_pattern(
    catalog: &dill::Catalog,
    dataset_ref_pattern: DatasetRefPattern,
) -> Result<DatasetRefPattern, CLIError> {
    match dataset_ref_pattern {
        DatasetRefPattern::Ref(dsr) => {
            let valid_ref = validate_dataset_ref(catalog, dsr)?;
            Ok(DatasetRefPattern::Ref(valid_ref))
        }
        DatasetRefPattern::Pattern(drp) => {
            let workspace_svc = catalog.get_one::<WorkspaceService>()?;
            if !workspace_svc.is_multi_tenant_workspace() && drp.account_name.is_some() {
                return Err(MultiTenantRefUnexpectedError {
                    dataset_ref_pattern: DatasetRefPattern::Pattern(drp),
                }
                .into());
            }
            Ok(DatasetRefPattern::Pattern(drp))
        }
    }
}

fn validate_many_dataset_refs<I>(
    catalog: &dill::Catalog,
    dataset_refs: I,
) -> Result<Vec<DatasetRef>, CLIError>
where
    I: IntoIterator<Item = DatasetRef>,
{
    let mut result_refs = Vec::new();
    for dataset_ref in dataset_refs {
        result_refs.push(validate_dataset_ref(catalog, dataset_ref)?);
    }

    Ok(result_refs)
}

fn validate_many_dataset_patterns<I>(
    catalog: &dill::Catalog,
    dataset_ref_patterns: I,
) -> Result<Vec<DatasetRefPattern>, CLIError>
where
    I: IntoIterator<Item = DatasetRefPattern>,
{
    dataset_ref_patterns
        .into_iter()
        .map(|p| validate_dataset_ref_pattern(catalog, p))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

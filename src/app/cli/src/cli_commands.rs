// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::CommandFactory as _;
use dill::Catalog;
use kamu::domain::TenancyConfig;
use kamu_accounts::CurrentAccountSubject;

use crate::cli::SystemApiServerSubCommand;
use crate::commands::*;
use crate::{accounts, cli, odf_server, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_command(
    base_catalog: &Catalog,
    cli_catalog: &Catalog,
    args: cli::Cli,
) -> Result<Box<dyn Command>, CLIError> {
    let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
    let tenancy_config = if workspace_svc.is_multi_tenant_workspace() {
        TenancyConfig::MultiTenant
    } else {
        TenancyConfig::SingleTenant
    };

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
            tenancy_config,
            cli_catalog.get_one()?,
        )),
        cli::Command::Complete(c) => {
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
                cli::Cli::command(),
                c.input,
                c.current,
            ))
        }
        cli::Command::Completions(c) => {
            Box::new(CompletionsCommand::new(cli::Cli::command(), c.shell))
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
            cli_catalog.get_one()?,
            c.all,
            c.recursive,
        )),
        cli::Command::Export(c) => Box::new(ExportCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.dataset,
            c.output_path,
            c.output_format,
            c.records_per_file,
            args.quiet,
        )),
        cli::Command::Ingest(c) => Box::new(IngestCommand::new(
            cli_catalog.get_one()?,
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
                    tenancy_config,
                ))
            }
        }
        cli::Command::Inspect(c) => match c.subcommand {
            cli::InspectSubCommand::Lineage(sc) => Box::new(InspectLineageCommand::new(
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
            let user_config = cli_catalog.get_one::<kamu_accounts::PredefinedAccountsConfig>()?;

            Box::new(ListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                accounts::AccountService::current_account_indication(
                    args.account,
                    tenancy_config,
                    user_config.as_ref(),
                ),
                accounts::AccountService::related_account_indication(
                    c.target_account,
                    c.all_accounts,
                ),
                cli_catalog.get_one()?,
                tenancy_config,
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
                cli_catalog.get_one()?,
                if c.user {
                    odf_server::AccessTokenStoreScope::User
                } else {
                    odf_server::AccessTokenStoreScope::Workspace
                },
                c.server.map(Into::into),
                c.access_token,
                c.check,
                c.repo_name,
                c.skip_add_repo,
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
            base_catalog.get_one()?,
            base_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.address,
            c.http_port,
            c.engine,
            c.env.unwrap_or_default(),
        )),
        cli::Command::Pull(c) => {
            if let Some(set_watermark) = c.set_watermark {
                Box::new(SetWatermarkCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    tenancy_config,
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
                    tenancy_config,
                    c.dataset.unwrap_or_default(),
                    cli_catalog.get_one()?,
                    c.all,
                    c.recursive,
                    c.fetch_uncacheable,
                    c.r#as,
                    !c.no_alias,
                    c.force,
                    c.reset_derivatives_on_diverged_input,
                    c.visibility.map(Into::into),
                ))
            }
        }
        cli::Command::Push(c) => Box::new(PushCommand::new(
            cli_catalog.get_one()?,
            cli_catalog.get_one()?,
            c.dataset.unwrap_or_default(),
            c.all,
            c.recursive,
            !c.no_alias,
            c.force,
            c.to,
            c.visibility.into(),
            cli_catalog.get_one()?,
            tenancy_config,
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
                cli_catalog.get_one()?,
                sc.repository.unwrap_or_default(),
                sc.all,
            )),
            cli::RepoSubCommand::List(_) => Box::new(RepositoryListCommand::new(
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
            )),
            cli::RepoSubCommand::Alias(sc) => match sc.subcommand {
                cli::RepoAliasSubCommand::Add(ssc) => Box::new(AliasAddCommand::new(
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    ssc.dataset,
                    ssc.alias,
                    ssc.pull,
                    ssc.push,
                )),
                cli::RepoAliasSubCommand::Delete(ssc) => Box::new(AliasDeleteCommand::new(
                    cli_catalog.get_one()?,
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
            cli_catalog.get_one()?,
            validate_dataset_ref(cli_catalog, c.dataset)?,
            c.hash,
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
                cli_catalog.get_one()?,
                c.command,
                c.url,
                c.engine,
                c.output_path,
                c.records_per_file,
            )),
            Some(cli::SqlSubCommand::Server(sc)) => Box::new(SqlServerCommand::new(
                base_catalog.get_one()?,
                base_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
                sc.address,
                sc.port,
                sc.engine,
                sc.livy,
            )),
        },
        cli::Command::System(c) => match c.subcommand {
            cli::SystemSubCommand::ApiServer(sc) => match sc.subcommand {
                None => Box::new(APIServerRunCommand::new(
                    base_catalog.clone(),
                    cli_catalog.clone(),
                    tenancy_config,
                    cli_catalog.get_one()?,
                    sc.address,
                    sc.http_port,
                    sc.external_address,
                    sc.get_token,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    cli_catalog.get_one()?,
                    args.e2e_output_data_path,
                )),
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
                cli_catalog.get_one()?,
                validate_many_dataset_patterns(cli_catalog, sc.dataset)?,
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
            cli::SystemSubCommand::Decode(sc) => {
                Box::new(SystemDecodeCommand::new(sc.manifest, sc.stdin))
            }
            cli::SystemSubCommand::E2e(sc) => Box::new(SystemE2ECommand::new(
                sc.action,
                sc.arguments.unwrap_or_default(),
                sc.dataset,
                cli_catalog.get_one()?,
                cli_catalog.get_one()?,
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
            let current_account_subject = cli_catalog.get_one::<CurrentAccountSubject>()?;

            let current_account_name = match current_account_subject.as_ref() {
                CurrentAccountSubject::Logged(l) => l.account_name.clone(),
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Cannot launch Web UI with anonymous account")
                }
            };

            Box::new(UICommand::new(
                base_catalog.clone(),
                tenancy_config,
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
            cli::SystemSubCommand::ApiServer(_) => false,
            _ => true,
        },
        cli::Command::Ui(_) => false,
        _ => true,
    }
}

pub fn command_needs_workspace(args: &cli::Cli) -> bool {
    match &args.command {
        cli::Command::Complete(_)
        | cli::Command::Completions(_)
        | cli::Command::Config(_)
        | cli::Command::Init(_)
        | cli::Command::New(_)
        | cli::Command::Version(_) => false,

        cli::Command::System(s) => match &s.subcommand {
            cli::SystemSubCommand::ApiServer(a) => match &a.subcommand {
                None | Some(SystemApiServerSubCommand::GqlQuery(_)) => true,
                Some(SystemApiServerSubCommand::GqlSchema(_)) => false,
            },
            cli::SystemSubCommand::DebugToken(_)
            | cli::SystemSubCommand::Decode(_)
            | cli::SystemSubCommand::Diagnose(_)
            | cli::SystemSubCommand::GenerateToken(_)
            | cli::SystemSubCommand::Info(_)
            | cli::SystemSubCommand::UpgradeWorkspace(_) => false,
            _ => true,
        },
        cli::Command::Login(l) => !l.user,
        _ => true,
    }
}

pub fn command_needs_startup_jobs(args: &cli::Cli) -> bool {
    if command_needs_workspace(args) {
        return true;
    }

    if let cli::Command::Complete(_) = &args.command {
        return true;
    }

    false
}

#[allow(clippy::match_like_matches_macro)]
pub fn command_needs_server_components(args: &cli::Cli) -> bool {
    match &args.command {
        cli::Command::System(c) => match &c.subcommand {
            cli::SystemSubCommand::ApiServer(_) => true,
            _ => false,
        },
        cli::Command::Ui(_) => true,
        _ => false,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Dataset reference validation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_dataset_ref(
    catalog: &dill::Catalog,
    dataset_ref: odf::DatasetRef,
) -> Result<odf::DatasetRef, CLIError> {
    if let odf::DatasetRef::Alias(ref alias) = dataset_ref {
        let workspace_svc = catalog.get_one::<WorkspaceService>()?;
        if !workspace_svc.is_multi_tenant_workspace() && alias.is_multi_tenant() {
            return Err(MultiTenantRefUnexpectedError {
                dataset_ref_pattern: odf::DatasetRefPattern::Ref(dataset_ref),
            }
            .into());
        }
    }
    Ok(dataset_ref)
}

fn validate_dataset_ref_pattern(
    catalog: &dill::Catalog,
    dataset_ref_pattern: odf::DatasetRefPattern,
) -> Result<odf::DatasetRefPattern, CLIError> {
    match dataset_ref_pattern {
        odf::DatasetRefPattern::Ref(dsr) => {
            let valid_ref = validate_dataset_ref(catalog, dsr)?;
            Ok(odf::DatasetRefPattern::Ref(valid_ref))
        }
        odf::DatasetRefPattern::Pattern(drp) => {
            let workspace_svc = catalog.get_one::<WorkspaceService>()?;
            if !workspace_svc.is_multi_tenant_workspace() && drp.account_name.is_some() {
                return Err(MultiTenantRefUnexpectedError {
                    dataset_ref_pattern: odf::DatasetRefPattern::Pattern(drp),
                }
                .into());
            }
            Ok(odf::DatasetRefPattern::Pattern(drp))
        }
    }
}

fn validate_many_dataset_refs<I>(
    catalog: &dill::Catalog,
    dataset_refs: I,
) -> Result<Vec<odf::DatasetRef>, CLIError>
where
    I: IntoIterator<Item = odf::DatasetRef>,
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
) -> Result<Vec<odf::DatasetRefPattern>, CLIError>
where
    I: IntoIterator<Item = odf::DatasetRefPattern>,
{
    dataset_ref_patterns
        .into_iter()
        .map(|p| validate_dataset_ref_pattern(catalog, p))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

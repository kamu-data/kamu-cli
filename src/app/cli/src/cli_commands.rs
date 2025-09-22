// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu::domain::TenancyConfig;
use kamu_accounts::CurrentAccountSubject;

use crate::accounts::CurrentAccountIndication;
use crate::cli::SystemApiServerSubCommand;
use crate::commands::*;
use crate::{WorkspaceService, accounts, cli, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_command(
    base_catalog: &Catalog,
    cli_catalog: &Catalog,
    args: cli::Cli,
    current_account_indication: CurrentAccountIndication,
) -> Result<Box<dyn TypedBuilder<dyn Command>>, CLIError> {
    let workspace_svc = cli_catalog.get_one::<WorkspaceService>()?;
    let tenancy_config = if workspace_svc.is_multi_tenant_workspace() {
        TenancyConfig::MultiTenant
    } else {
        TenancyConfig::SingleTenant
    };

    let command: Box<dyn TypedBuilder<dyn Command>> = match args.command {
        cli::Command::Add(c) => Box::new(
            AddCommand::builder(
                c.manifest,
                c.name,
                c.recursive,
                c.replace,
                c.stdin,
                c.visibility
                    .map(Into::into)
                    .unwrap_or(tenancy_config.default_dataset_visibility()),
            )
            .cast(),
        ),

        cli::Command::Complete(c) => Box::new(CompleteCommand::builder(c.input, c.current).cast()),

        cli::Command::Completions(c) => Box::new(CompletionsCommand::builder(c.shell).cast()),

        cli::Command::Config(c) => match c.subcommand {
            cli::ConfigSubCommand::List(sc) => {
                Box::new(ConfigListCommand::builder(sc.user, sc.with_defaults).cast())
            }
            cli::ConfigSubCommand::Get(sc) => {
                Box::new(ConfigGetCommand::builder(sc.user, sc.with_defaults, sc.cfgkey).cast())
            }
            cli::ConfigSubCommand::Set(sc) => {
                Box::new(ConfigSetCommand::builder(sc.user, sc.cfgkey, sc.value).cast())
            }
        },

        cli::Command::Delete(c) => Box::new(
            DeleteCommand::builder(
                validate_many_dataset_patterns(cli_catalog, c.dataset)?,
                c.all,
                c.recursive,
            )
            .cast(),
        ),

        cli::Command::Export(c) => Box::new(
            ExportCommand::builder(
                c.dataset,
                c.output_path,
                c.output_format,
                c.records_per_file,
                args.quiet,
            )
            .cast(),
        ),

        cli::Command::Ingest(c) => Box::new(
            IngestCommand::builder(
                validate_dataset_ref(cli_catalog, c.dataset)?,
                c.file.unwrap_or_default(),
                c.source_name,
                c.event_time,
                c.stdin,
                c.recursive,
                c.input_format,
            )
            .cast(),
        ),

        cli::Command::Init(c) => {
            if c.pull_images {
                Box::new(PullImagesCommand::builder(c.list_only).cast())
            } else {
                Box::new(InitCommand::builder(c.exists_ok).cast())
            }
        }

        cli::Command::Inspect(c) => match c.subcommand {
            cli::InspectSubCommand::Lineage(sc) => Box::new(
                InspectLineageCommand::builder(
                    validate_many_dataset_refs(cli_catalog, sc.dataset)?,
                    sc.browse,
                    sc.output_format,
                )
                .cast(),
            ),
            cli::InspectSubCommand::Query(sc) => Box::new(
                InspectQueryCommand::builder(validate_dataset_ref(cli_catalog, sc.dataset)?).cast(),
            ),
            cli::InspectSubCommand::Schema(sc) => Box::new(
                InspectSchemaCommand::builder(
                    validate_dataset_ref(cli_catalog, sc.dataset)?,
                    sc.output_format,
                    sc.from_data_file,
                )
                .cast(),
            ),
        },

        cli::Command::List(c) => Box::new(
            ListCommand::builder(
                current_account_indication,
                accounts::AccountService::related_account_indication(
                    c.target_account,
                    c.all_accounts,
                ),
                cli_catalog.get_one()?,
                c.wide,
            )
            .cast(),
        ),

        cli::Command::Log(c) => Box::new(
            LogCommand::builder(
                validate_dataset_ref(cli_catalog, c.dataset)?,
                c.output_format,
                c.filter,
                c.limit,
            )
            .cast(),
        ),

        cli::Command::Login(c) => match c.subcommand {
            Some(cli::LoginSubCommand::Oauth(sc)) => Box::new(
                LoginSilentCommand::builder(
                    if c.user {
                        odf_server::AccessTokenStoreScope::User
                    } else {
                        odf_server::AccessTokenStoreScope::Workspace
                    },
                    sc.server.map(Into::into),
                    LoginSilentMode::OAuth(LoginSilentModeOAuth {
                        provider: sc.provider,
                        access_token: sc.access_token,
                    }),
                    c.repo_name,
                    c.skip_add_repo,
                )
                .cast(),
            ),
            Some(cli::LoginSubCommand::Password(sc)) => Box::new(
                LoginSilentCommand::builder(
                    if c.user {
                        odf_server::AccessTokenStoreScope::User
                    } else {
                        odf_server::AccessTokenStoreScope::Workspace
                    },
                    sc.server.map(Into::into),
                    LoginSilentMode::Password(LoginSilentModePassword {
                        login: sc.login,
                        password: sc.password,
                    }),
                    c.repo_name,
                    c.skip_add_repo,
                )
                .cast(),
            ),
            None => Box::new(
                LoginCommand::builder(
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
                    c.predefined_odf_backend_url.map(Into::into),
                )
                .cast(),
            ),
        },

        cli::Command::Logout(c) => Box::new(
            LogoutCommand::builder(
                if c.user {
                    odf_server::AccessTokenStoreScope::User
                } else {
                    odf_server::AccessTokenStoreScope::Workspace
                },
                c.server.map(Into::into),
                c.all,
            )
            .cast(),
        ),

        cli::Command::New(c) => {
            Box::new(NewDatasetCommand::builder(c.name, c.root, c.derivative, None).cast())
        }

        cli::Command::Notebook(c) => Box::new(
            NotebookCommand::builder(
                c.address,
                c.http_port,
                c.engine,
                c.env
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| match s.find('=') {
                        None => (s, None),
                        Some(pos) => {
                            let (name, value) = s.split_at(pos);
                            (name.to_string(), Some(value[1..].to_string()))
                        }
                    })
                    .collect(),
                base_catalog.get_one()?,
                base_catalog.get_one()?,
            )
            .cast(),
        ),

        cli::Command::Pull(c) => {
            if let Some(set_watermark) = c.set_watermark {
                Box::new(
                    SetWatermarkCommand::builder(
                        c.dataset.unwrap_or_default(),
                        c.all,
                        c.recursive,
                        set_watermark,
                    )
                    .cast(),
                )
            } else {
                Box::new(
                    PullCommand::builder(
                        c.dataset.unwrap_or_default(),
                        c.all,
                        c.recursive,
                        c.fetch_uncacheable,
                        c.r#as,
                        !c.no_alias,
                        c.force,
                        c.reset_derivatives_on_diverged_input,
                        c.visibility
                            .map(Into::into)
                            .unwrap_or(tenancy_config.default_dataset_visibility()),
                    )
                    .cast(),
                )
            }
        }

        cli::Command::Push(c) => Box::new(
            PushCommand::builder(
                c.dataset.unwrap_or_default(),
                c.all,
                c.recursive,
                !c.no_alias,
                c.force,
                c.to,
                c.visibility.map(Into::into),
                cli_catalog.get_one()?,
            )
            .cast(),
        ),

        cli::Command::Rename(c) => Box::new(
            RenameCommand::builder(validate_dataset_ref(cli_catalog, c.dataset)?, c.name).cast(),
        ),

        cli::Command::Repo(c) => match c.subcommand {
            cli::RepoSubCommand::Add(sc) => {
                Box::new(RepositoryAddCommand::builder(sc.name, sc.url).cast())
            }
            cli::RepoSubCommand::Delete(sc) => Box::new(
                RepositoryDeleteCommand::builder(sc.repository.unwrap_or_default(), sc.all).cast(),
            ),
            cli::RepoSubCommand::List(_) => Box::new(RepositoryListCommand::builder().cast()),
            cli::RepoSubCommand::Alias(sc) => match sc.subcommand {
                cli::RepoAliasSubCommand::Add(ssc) => Box::new(
                    AliasAddCommand::builder(ssc.dataset, ssc.alias, ssc.pull, ssc.push).cast(),
                ),
                cli::RepoAliasSubCommand::Delete(ssc) => Box::new(
                    AliasDeleteCommand::builder(
                        ssc.dataset,
                        ssc.alias,
                        ssc.all,
                        ssc.pull,
                        ssc.push,
                    )
                    .cast(),
                ),
                cli::RepoAliasSubCommand::List(ssc) => {
                    Box::new(AliasListCommand::builder(ssc.dataset).cast())
                }
            },
        },

        cli::Command::Reset(c) => Box::new(
            ResetCommand::builder(validate_dataset_ref(cli_catalog, c.dataset)?, c.hash).cast(),
        ),

        cli::Command::Search(c) => Box::new(
            SearchCommand::builder(c.query, c.repo.unwrap_or_default(), c.local, c.max_results)
                .cast(),
        ),

        cli::Command::Sql(c) => match c.subcommand {
            None => Box::new(
                SqlShellCommand::builder(
                    c.command,
                    c.url,
                    c.engine,
                    c.output_path,
                    c.records_per_file,
                )
                .cast(),
            ),
            Some(cli::SqlSubCommand::Server(sc)) => Box::new(
                SqlServerCommand::builder(
                    sc.address,
                    sc.port,
                    sc.engine,
                    sc.livy,
                    base_catalog.get_one()?,
                    base_catalog.get_one()?,
                )
                .cast(),
            ),
        },

        cli::Command::System(c) => match c.subcommand {
            cli::SystemSubCommand::ApiServer(sc) => match sc.subcommand {
                None => Box::new(
                    APIServerRunCommand::builder(
                        sc.address,
                        sc.http_port,
                        sc.external_address,
                        sc.get_token,
                        args.e2e_output_data_path,
                        base_catalog.clone(),
                        cli_catalog.clone(),
                    )
                    .cast(),
                ),
                Some(cli::SystemApiServerSubCommand::GqlQuery(ssc)) => Box::new(
                    APIServerGqlQueryCommand::builder(cli_catalog.clone(), ssc.query, ssc.full)
                        .cast(),
                ),
                Some(cli::SystemApiServerSubCommand::GqlSchema(_)) => {
                    Box::new(APIServerGqlSchemaCommand::builder().cast())
                }
            },
            cli::SystemSubCommand::Compact(sc) => Box::new(
                CompactCommand::builder(
                    validate_many_dataset_patterns(cli_catalog, sc.dataset)?,
                    sc.max_slice_size,
                    sc.max_slice_records,
                    sc.hard,
                    sc.verify,
                    sc.keep_metadata_only,
                )
                .cast(),
            ),
            cli::SystemSubCommand::Diagnose(_) => Box::new(SystemDiagnoseCommand::builder().cast()),
            cli::SystemSubCommand::DebugToken(sc) => {
                Box::new(DebugTokenCommand::builder(sc.token).cast())
            }
            cli::SystemSubCommand::Decode(sc) => {
                Box::new(SystemDecodeCommand::builder(sc.manifest, sc.stdin).cast())
            }
            cli::SystemSubCommand::E2e(sc) => Box::new(
                SystemE2ECommand::builder(sc.action, sc.arguments.unwrap_or_default(), sc.dataset)
                    .cast(),
            ),
            cli::SystemSubCommand::Gc(_) => Box::new(GcCommand::builder().cast()),
            cli::SystemSubCommand::GenerateToken(sc) => Box::new(
                GenerateTokenCommand::builder(sc.login, sc.subject, sc.expiration_time_sec).cast(),
            ),
            cli::SystemSubCommand::Info(sc) => {
                Box::new(SystemInfoCommand::builder(sc.output_format).cast())
            }
            cli::SystemSubCommand::Ipfs(sc) => match sc.subcommand {
                cli::SystemIpfsSubCommand::Add(ssc) => {
                    Box::new(SystemIpfsAddCommand::builder(ssc.dataset).cast())
                }
            },
            cli::SystemSubCommand::UpgradeWorkspace(_) => {
                Box::new(UpgradeWorkspaceCommand::builder().cast())
            }
        },

        cli::Command::Tail(c) => Box::new(
            TailCommand::builder(
                validate_dataset_ref(cli_catalog, c.dataset)?,
                c.skip_records,
                c.num_records,
            )
            .cast(),
        ),

        cli::Command::Ui(c) => {
            let current_account_subject = cli_catalog.get_one::<CurrentAccountSubject>()?;

            let current_account_name = match current_account_subject.as_ref() {
                CurrentAccountSubject::Logged(l) => l.account_name.clone(),
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Cannot launch Web UI with anonymous account")
                }
            };

            Box::new(
                UICommand::builder(
                    current_account_name,
                    c.address,
                    c.http_port,
                    c.get_token,
                    base_catalog.clone(),
                )
                .cast(),
            )
        }

        cli::Command::Verify(c) => Box::new(
            VerifyCommand::builder(
                validate_many_dataset_patterns(cli_catalog, c.dataset)?,
                c.recursive,
                c.integrity,
            )
            .cast(),
        ),

        cli::Command::Version(c) => Box::new(VersionCommand::builder(c.output_format).cast()),
    };

    Ok(command)
}

#[allow(clippy::match_like_matches_macro)]
pub fn command_needs_transaction(args: &cli::Cli) -> bool {
    match &args.command {
        cli::Command::System(c) => match &c.subcommand {
            cli::SystemSubCommand::ApiServer(sas) => {
                matches!(
                    &sas.subcommand,
                    Some(SystemApiServerSubCommand::GqlQuery(_))
                )
            }
            _ => true,
        },
        cli::Command::Ui(_) | cli::Command::Login(_) | cli::Command::Pull(_) => false,
        _ => true,
    }
}

pub fn command_needs_outbox_processing(args: &cli::Cli) -> bool {
    match &args.command {
        cli::Command::System(c) => match &c.subcommand {
            cli::SystemSubCommand::Info(_)
            | cli::SystemSubCommand::UpgradeWorkspace(_)
            | cli::SystemSubCommand::DebugToken(_)
            | cli::SystemSubCommand::Decode(_)
            | cli::SystemSubCommand::GenerateToken(_)
            | cli::SystemSubCommand::Gc(_) => false,
            cli::SystemSubCommand::ApiServer(a) => match &a.subcommand {
                None | Some(SystemApiServerSubCommand::GqlQuery(_)) => true,
                Some(SystemApiServerSubCommand::GqlSchema(_)) => false,
            },
            _ => true,
        },
        cli::Command::Complete(_)
        | cli::Command::Completions(_)
        | cli::Command::Config(_)
        | cli::Command::New(_)
        | cli::Command::Sql(_)
        | cli::Command::Version(_)
        | cli::Command::Notebook(_) => false,
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
    // ToDo: Revisit and decide do all commands that require workspace
    // also require startup jobs
    if command_needs_workspace(args) {
        return true;
    }

    match &args.command {
        cli::Command::Complete(_) => true,
        cli::Command::Init(c) if !c.pull_images => {
            // NOTE: When initializing, we need to run initialization at least to create
            //       user accounts.
            true
        }
        _ => false,
    }
}

#[expect(clippy::match_like_matches_macro)]
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

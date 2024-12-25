// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use console::style as s;
use container_runtime::ContainerRuntime;
use internal_error::*;

use super::common::PullImageProgress;
use super::{CLIError, Command, SqlShellEngine};
use crate::explore::{FlightSqlServiceFactory, NotebookServerFactory, SparkLivyServerFactory};
use crate::output::OutputConfig;

pub struct NotebookCommand {
    flight_sql_service_factory: Arc<FlightSqlServiceFactory>,
    spark_livy_server_factory: Arc<SparkLivyServerFactory>,
    notebook_server_factory: Arc<NotebookServerFactory>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: Option<IpAddr>,
    port: Option<u16>,
    engine: Option<SqlShellEngine>,
    env_vars: Vec<(String, Option<String>)>,
}

impl NotebookCommand {
    pub fn new<Iter, Str>(
        flight_sql_service_factory: Arc<FlightSqlServiceFactory>,
        spark_livy_server_factory: Arc<SparkLivyServerFactory>,
        notebook_server_factory: Arc<NotebookServerFactory>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: Option<IpAddr>,
        port: Option<u16>,
        engine: Option<SqlShellEngine>,
        env_vars: Iter,
    ) -> Self
    where
        Iter: IntoIterator<Item = Str>,
        Str: AsRef<str>,
    {
        Self {
            flight_sql_service_factory,
            spark_livy_server_factory,
            notebook_server_factory,
            output_config,
            container_runtime,
            address,
            port,
            engine,
            env_vars: env_vars
                .into_iter()
                .map(|elem| {
                    let s = elem.as_ref();
                    match s.find('=') {
                        None => (s.to_owned(), None),
                        Some(pos) => {
                            let (name, value) = s.split_at(pos);
                            (name.to_owned(), Some(value[1..].to_owned()))
                        }
                    }
                })
                .collect(),
        }
    }

    fn collect_env_vars(&self) -> Result<Vec<(String, String)>, CLIError> {
        self.env_vars
            .iter()
            .map(|(name, value)| {
                value
                    .clone()
                    .or_else(|| std::env::var(name).ok())
                    .ok_or_else(|| {
                        CLIError::usage_error(format!("Environment variable {name} is not set"))
                    })
                    .map(|v| (name.to_owned(), v))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn startup_spinner(&self) -> Option<indicatif::ProgressBar> {
        if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message("Starting Jupyter server");
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        }
    }

    async fn run_datafusion(&mut self) -> Result<(), CLIError> {
        let environment_vars = self.collect_env_vars()?;

        let pull_progress = PullImageProgress::new(self.output_config.clone(), "Jupyter");

        self.notebook_server_factory
            .ensure_image(Some(&pull_progress))
            .await
            .int_err()?;

        let spinner = self.startup_spinner();

        // FIXME: We have to bind FlightSQL to 0.0.0.0 external interface instead of
        // 127.0.0.1 as Jupyter will be connection from the outside
        let flight_sql_svc = self
            .flight_sql_service_factory
            .start(Some(std::net::Ipv4Addr::UNSPECIFIED.into()), None)
            .await?;

        let client_url = url::Url::parse(&format!(
            "grpc://host.docker.internal:{}",
            flight_sql_svc.local_addr().port()
        ))
        .unwrap();

        let mut notebook_container = self
            .notebook_server_factory
            .start(
                &client_url,
                self.address,
                self.port,
                None,
                environment_vars,
                self.output_config.verbosity_level > 0,
                move |url| {
                    if let Some(s) = spinner {
                        s.finish_and_clear();
                    }
                    eprintln!(
                        "{}\n  {}",
                        s("Jupyter server is now running at:").green().bold(),
                        s(url).bold(),
                    );
                    eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
                    let _ = webbrowser::open(url);
                },
            )
            .await
            .int_err()?;

        tokio::select! {
            _ = container_runtime::signal::graceful_stop() => {
                eprintln!("{}", s("Shutting down").yellow());
            }
            _ = flight_sql_svc.wait() => {
                tracing::warn!("FlightSQL server terminated");
                eprintln!("{}", s("FlightSQL server terminated").yellow());
            }
            exit_status = notebook_container.wait() => {
                tracing::warn!(?exit_status, "Notebook server terminated");
                eprintln!("{}", s("Notebook server terminated").yellow());
            }
        }

        notebook_container.terminate().await.int_err()?;

        Ok(())
    }

    async fn run_spark(&mut self) -> Result<(), CLIError> {
        let environment_vars = self.collect_env_vars()?;

        // Pull images
        self.spark_livy_server_factory
            .ensure_image(Some(&PullImageProgress::new(
                self.output_config.clone(),
                "Spark",
            )))
            .await
            .int_err()?;

        self.notebook_server_factory
            .ensure_image(Some(&PullImageProgress::new(
                self.output_config.clone(),
                "Jupyter",
            )))
            .await
            .int_err()?;

        // Start containers on one network
        let spinner = self.startup_spinner();

        let network = self
            .container_runtime
            .create_random_network_with_prefix("kamu-")
            .await
            .int_err()?;

        let mut livy = self
            .spark_livy_server_factory
            .start(
                None,
                None,
                self.output_config.verbosity_level > 0,
                Some(network.name()),
            )
            .await
            .int_err()?;

        let mut notebook = self
            .notebook_server_factory
            .start(
                &url::Url::parse("http://kamu-livy:8998").unwrap(),
                self.address,
                self.port,
                Some(network.name()),
                environment_vars,
                self.output_config.verbosity_level > 0,
                move |url| {
                    if let Some(s) = spinner {
                        s.finish_and_clear();
                    }
                    eprintln!(
                        "{}\n  {}",
                        s("Jupyter server is now running at:").green().bold(),
                        s(url).bold(),
                    );
                    eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
                    let _ = webbrowser::open(url);
                },
            )
            .await
            .int_err()?;

        tokio::select! {
            _ = container_runtime::signal::graceful_stop() => {
                eprintln!("{}", s("Shutting down").yellow());
            },
            exit_status = livy.wait() => {
                tracing::warn!(?exit_status, "Livy container exited");
            },
            exit_status = notebook.wait() => {
                tracing::warn!(?exit_status, "Jupyter container exited");
            },
        }

        notebook.terminate().await.int_err()?;
        livy.terminate().await.int_err()?;
        network.free().await.int_err()?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for NotebookCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let engine = self.engine.unwrap_or(SqlShellEngine::Datafusion);

        match engine {
            SqlShellEngine::Datafusion => self.run_datafusion().await,
            SqlShellEngine::Spark => self.run_spark().await,
        }
    }
}

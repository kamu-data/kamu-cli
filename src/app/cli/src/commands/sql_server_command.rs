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
use kamu::*;

use super::common::PullImageProgress;
use super::{CLIError, Command, SqlShellEngine};
use crate::explore::{FlightSqlServiceFactory, SparkLivyServerFactory, SqlShellImpl};
use crate::output::*;
use crate::WorkspaceLayout;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlServerCommand {
    flight_sql_service_factory: Arc<FlightSqlServiceFactory>,
    spark_livy_server_factory: Arc<SparkLivyServerFactory>,
    workspace_layout: Arc<WorkspaceLayout>,
    engine_prov_config: Arc<EngineProvisionerLocalConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: Option<IpAddr>,
    port: Option<u16>,
    engine: Option<SqlShellEngine>,
    livy: bool,
}

impl SqlServerCommand {
    pub fn new(
        flight_sql_service_factory: Arc<FlightSqlServiceFactory>,
        spark_livy_server_factory: Arc<SparkLivyServerFactory>,
        workspace_layout: Arc<WorkspaceLayout>,
        engine_prov_config: Arc<EngineProvisionerLocalConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: Option<IpAddr>,
        port: Option<u16>,
        engine: Option<SqlShellEngine>,
        livy: bool,
    ) -> Self {
        Self {
            flight_sql_service_factory,
            spark_livy_server_factory,
            workspace_layout,
            engine_prov_config,
            output_config,
            container_runtime,
            address,
            port,
            engine,
            livy,
        }
    }

    fn startup_spinner(&self, message: &str) -> Option<indicatif::ProgressBar> {
        if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message(message.to_string());
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        }
    }

    async fn run_datafusion_flight_sql(&mut self) -> Result<(), CLIError> {
        let flight_sql_svc = self
            .flight_sql_service_factory
            .start(self.address, self.port)
            .await?;

        eprintln!(
            "{} {}",
            s("Flight SQL server is now running on:").green().bold(),
            s(format!("{}", flight_sql_svc.local_addr())).bold(),
        );
        eprintln!(
            "{}",
            s("Protocol documentation: https://docs.kamu.dev/node/protocols/flight-sql/").dim()
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        flight_sql_svc.wait().await.int_err()?;

        Ok(())
    }

    async fn run_spark_jdbc(&mut self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(
            self.container_runtime.clone(),
            self.engine_prov_config.spark_image.clone(),
        );

        let mut pull_progress = PullImageProgress::new(self.output_config.clone(), "engine");
        sql_shell
            .ensure_images(&mut pull_progress)
            .await
            .int_err()?;

        let spinner = self.startup_spinner("Starting Spark JDBC server");

        let address = self.address.unwrap_or("127.0.0.1".parse().unwrap());
        let port = self.port.unwrap_or(10000);

        let mut spark = sql_shell
            .run_server(
                &self.workspace_layout,
                Vec::new(),
                Some(&address),
                Some(port),
            )
            .await
            .int_err()?;

        if let Some(s) = spinner {
            s.finish_and_clear();
        }
        eprintln!(
            "{} {}",
            s("Spark JDBC server is now running on:").green().bold(),
            s(format!("jdbc:hive2://{address}:{port}")).bold(),
        );
        eprintln!(
            "{}",
            s("Protocol documentation: https://docs.kamu.dev/node/protocols/jdbc/").dim()
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        tokio::select! {
            _ = container_runtime::signal::graceful_stop() => {
                eprintln!("{}", s("Shutting down").yellow());
                spark.terminate().await?;
            },
            exit_status = spark.wait() => {
                exit_status?;
                eprintln!("{}", s("Container exited").yellow());
            },
        }

        Ok(())
    }

    async fn run_spark_livy(&mut self) -> Result<(), CLIError> {
        let pull_progress = PullImageProgress::new(self.output_config.clone(), "engine");
        self.spark_livy_server_factory
            .ensure_image(Some(&pull_progress))
            .await
            .int_err()?;

        let spinner = self.startup_spinner("Starting Spark Livy server");

        let mut livy = self
            .spark_livy_server_factory
            .start(
                self.address,
                self.port,
                self.output_config.verbosity_level > 0,
                None,
            )
            .await
            .int_err()?;

        livy.wait_for_socket(std::time::Duration::from_secs(30))
            .await
            .int_err()?;

        if let Some(s) = spinner {
            s.finish_and_clear();
        }
        eprintln!(
            "{} {}",
            s("Spark Livy server is now running on:").green().bold(),
            s(format!("http://{}", livy.local_addr())).bold(),
        );
        eprintln!(
            "{}",
            s("This protocol is deprecated and will be replaced by FlightSQL").yellow()
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        tokio::select! {
            _ = container_runtime::signal::graceful_stop() => {
                eprintln!("{}", s("Shutting down").yellow());
                livy.terminate().await?;
            },
            exit_status = livy.wait() => {
                exit_status?;
                eprintln!("{}", s("Container exited").yellow());
            },
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let engine = self.engine.unwrap_or(if !self.livy {
            SqlShellEngine::Datafusion
        } else {
            SqlShellEngine::Spark
        });

        match engine {
            SqlShellEngine::Datafusion => self.run_datafusion_flight_sql().await,
            SqlShellEngine::Spark if self.livy => self.run_spark_livy().await,
            SqlShellEngine::Spark => self.run_spark_jdbc().await,
        }
    }
}

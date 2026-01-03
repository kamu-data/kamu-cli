// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use database_common::DatabaseTransactionRunner;
use dill::{Builder, BuilderExt, Catalog, TypecastBuilder};
use internal_error::InternalError;
use petgraph::algo::toposort;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableDiGraph;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type JobName = &'static str;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait InitOnStartup: Send + Sync {
    /// Performs initialization procedure.
    /// The operation is expected to be idempotent
    async fn run_initialization(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct InitOnStartupMeta {
    pub job_name: JobName,
    pub depends_on: &'static [JobName],
    pub requires_transaction: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum StartupJobsError {
    #[error(transparent)]
    JobNameNonUnique(StartupJobsNonUniqueNameError),

    #[error(transparent)]
    DependsOnUnresolved(StartupJobsDependsOnUnresolvedError),

    #[error(transparent)]
    DependsOnLoop(StartupJobsDependsOnLoopError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Startup job name '{job_name}' is not unique")]
pub struct StartupJobsNonUniqueNameError {
    pub job_name: JobName,
}

#[derive(Error, Debug)]
#[error("Startup job name '{job_name}' depends on unresolved job'{unresolved_depends_on}'")]
pub struct StartupJobsDependsOnUnresolvedError {
    pub job_name: JobName,
    pub unresolved_depends_on: JobName,
}

#[derive(Error, Debug)]
#[error("Startup job name '{job_name}' forms a dependency loop on itself")]
pub struct StartupJobsDependsOnLoopError {
    pub job_name: JobName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder, Default)]
pub struct RunStartupJobsOptions {
    pub job_selector: Option<JobSelector>,
}

#[derive(Debug)]
pub enum JobSelector {
    AllOf(HashSet<JobName>),
    NoneOf(HashSet<JobName>),
}

impl JobSelector {
    pub fn matches(&self, job_name: JobName) -> bool {
        match self {
            Self::AllOf(required) => required.contains(job_name),
            Self::NoneOf(forbidden) => !forbidden.contains(job_name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all)]
pub async fn run_startup_jobs(catalog: &Catalog) -> Result<(), StartupJobsError> {
    run_startup_jobs_ex(catalog, RunStartupJobsOptions::default()).await
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn run_startup_jobs_ex(
    catalog: &Catalog,
    options: RunStartupJobsOptions,
) -> Result<(), StartupJobsError> {
    let job_builders_by_name = {
        let mut job_builders_by_name = HashMap::new();

        let startup_job_builders = catalog.builders_for::<dyn InitOnStartup>();
        for startup_job_builder in startup_job_builders {
            let metadata = get_startup_job_metadata(&startup_job_builder);
            let job_name = metadata.job_name;

            if job_builders_by_name
                .insert(job_name, (startup_job_builder, metadata))
                .is_some()
            {
                return Err(StartupJobsError::JobNameNonUnique(
                    StartupJobsNonUniqueNameError { job_name },
                ));
            }
        }

        job_builders_by_name
    };
    tracing::debug!("Defined {} startup jobs", job_builders_by_name.len());

    if let Some(job_selector) = &options.job_selector {
        match job_selector {
            JobSelector::AllOf(required) => {
                tracing::debug!("Selecting startup jobs: {:?}", required);
            }
            JobSelector::NoneOf(forbidden) => {
                tracing::debug!("Skipping startup jobs: {:?}", forbidden);
            }
        }
    }

    check_startup_job_dependencies(&job_builders_by_name)?;
    tracing::debug!("Startup job dependencies checked");

    let topological_order = jobs_topological_order(&build_job_graph(&job_builders_by_name))?;
    tracing::debug!("Topological order of startup jobs: {topological_order:?}");

    for job_name in topological_order {
        if let Some(job_selector) = &options.job_selector
            && !job_selector.matches(job_name)
        {
            continue;
        }

        let (job_builder, job_metadata) = job_builders_by_name
            .get(job_name)
            .expect("Job builder must be present");

        if job_metadata.requires_transaction {
            DatabaseTransactionRunner::new(catalog.clone())
                .transactional(|transaction_catalog| async move {
                    let job = job_builder.get(&transaction_catalog).unwrap();
                    job.run_initialization().await
                })
                .await?;
        } else {
            let job = job_builder.get(catalog).unwrap();
            job.run_initialization().await?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_startup_job_metadata<'a>(
    startup_job_builder: &TypecastBuilder<'a, dyn InitOnStartup + 'static>,
) -> InitOnStartupMeta {
    let all_metadata: Vec<&InitOnStartupMeta> = startup_job_builder.metadata_get_all();
    assert!(
        all_metadata.len() == 1,
        "Must define exatly one `InitOnStartupMeta` record for a startup job{}",
        startup_job_builder.instance_type().name
    );
    (*all_metadata.first().unwrap()).clone()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn check_startup_job_dependencies(
    job_builders_by_name: &HashMap<
        &'static str,
        (
            TypecastBuilder<'_, dyn InitOnStartup + 'static>,
            InitOnStartupMeta,
        ),
    >,
) -> Result<(), StartupJobsError> {
    for (_, job_metadata) in job_builders_by_name.values() {
        for depends_on in job_metadata.depends_on {
            if !job_builders_by_name.contains_key(depends_on) {
                return Err(StartupJobsError::DependsOnUnresolved(
                    StartupJobsDependsOnUnresolvedError {
                        job_name: job_metadata.job_name,
                        unresolved_depends_on: depends_on,
                    },
                ));
            }
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn build_job_graph(
    job_builders_by_name: &HashMap<
        &'static str,
        (
            TypecastBuilder<'_, dyn InitOnStartup + 'static>,
            InitOnStartupMeta,
        ),
    >,
) -> StableDiGraph<&'static str, ()> {
    let mut job_graph: StableDiGraph<&'static str, ()> = StableDiGraph::new();
    let mut job_node_indices: HashMap<&'static str, NodeIndex> = HashMap::new();

    for (_, job_metadata) in job_builders_by_name.values() {
        let node_index = job_graph.add_node(job_metadata.job_name);
        job_node_indices.insert(job_metadata.job_name, node_index);
    }

    for (_, job_metadata) in job_builders_by_name.values() {
        let node_index = job_node_indices
            .get(job_metadata.job_name)
            .expect("Node must be indexed");

        for depends_on in job_metadata.depends_on {
            let dependency_index = job_node_indices
                .get(depends_on)
                .expect("Node must be indexed");

            job_graph.add_edge(*dependency_index, *node_index, ());
        }
    }

    job_graph
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn jobs_topological_order(
    job_graph: &StableDiGraph<&'static str, ()>,
) -> Result<Vec<&'static str>, StartupJobsError> {
    let sort_result = toposort(job_graph, None);
    let topological_order: Vec<_> = match sort_result {
        Ok(nodes_order) => nodes_order
            .iter()
            .map(|node_index| {
                *(job_graph
                    .node_weight(*node_index)
                    .expect("Node must be present"))
            })
            .collect(),
        Err(cycle) => {
            let looped_job_name = *(job_graph
                .node_weight(cycle.node_id())
                .expect("Node must be present"));
            return Err(StartupJobsError::DependsOnLoop(
                StartupJobsDependsOnLoopError {
                    job_name: looped_job_name,
                },
            ));
        }
    };
    Ok(topological_order)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

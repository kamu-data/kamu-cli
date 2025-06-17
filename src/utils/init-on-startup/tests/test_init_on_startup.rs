// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use dill::*;
use init_on_startup::{
    InitOnStartup,
    InitOnStartupMeta,
    RunStartupJobsOptions,
    StartupJobsError,
    run_startup_jobs,
    run_startup_jobs_ex,
};
use internal_error::InternalError;
use pretty_assertions::{assert_eq, assert_matches};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct JobExecutions {
    job_names: Arc<Mutex<Vec<&'static str>>>,
}

#[component(pub)]
#[scope(Singleton)]
impl JobExecutions {
    pub fn new() -> Self {
        Self {
            job_names: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn add_job(&self, job_name: &'static str) {
        self.job_names.lock().unwrap().push(job_name);
    }

    pub fn job_names(&self) -> Vec<&'static str> {
        let inner = &*self.job_names.lock().unwrap();
        inner.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_startup_job {
    ($job_suffix: ident, $depends_on: expr) => {
        paste::paste! {
            test_startup_job!($job_suffix, concat!("TestJob", stringify!($job_suffix)), $depends_on);
        }
    };
    ($job_suffix: ident, $job_name: expr, $depends_on: expr) => {
        paste::paste! {
            struct [<"TestJob" $job_suffix>] {
                job_executions: Arc<JobExecutions>,
            }

            #[component(pub)]
            #[interface(dyn InitOnStartup)]
            #[meta(InitOnStartupMeta {
                job_name: $job_name,
                requires_transaction: false,
                depends_on: $depends_on,
            })]
            #[scope(Singleton)]
            impl [<"TestJob" $job_suffix>] {
                fn new(job_executions: Arc<JobExecutions>) -> Self {
                    Self {
                        job_executions,
                    }
                }
            }

            #[async_trait::async_trait]
            impl InitOnStartup for [<"TestJob" $job_suffix>] {
                async fn run_initialization(&self) -> Result<(), InternalError> {
                    self.job_executions.add_job(concat!("TestJob", stringify!($job_suffix)));
                    Ok(())
                }
            }
        };
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_independent_jobs() {
    test_startup_job!(A, &[]);
    test_startup_job!(B, &[]);
    test_startup_job!(C, &[]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    run_startup_jobs(&catalog).await.unwrap();

    // The order of executions is random, but all 3 must be present
    let executions = catalog.get_one::<JobExecutions>().unwrap();
    assert_eq!(
        ["TestJobA", "TestJobB", "TestJobC"]
            .into_iter()
            .collect::<HashSet<_>>(),
        executions.job_names().into_iter().collect(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_independent_jobs_skip_completed() {
    test_startup_job!(A, &[]);
    test_startup_job!(B, &[]);
    test_startup_job!(C, &[]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    run_startup_jobs_ex(
        &catalog,
        RunStartupJobsOptions::builder()
            .skip_completed_jobs(HashSet::from(["TestJobA"]))
            .build(),
    )
    .await
    .unwrap();

    let executions = catalog.get_one::<JobExecutions>().unwrap();
    assert_eq!(
        ["TestJobB", "TestJobC"].into_iter().collect::<HashSet<_>>(),
        executions.job_names().into_iter().collect(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_linear_dependency() {
    test_startup_job!(A, &[]);
    test_startup_job!(B, &["TestJobA"]);
    test_startup_job!(C, &["TestJobB"]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    run_startup_jobs(&catalog).await.unwrap();

    // The order of executions must respect dependencies
    let executions = catalog.get_one::<JobExecutions>().unwrap();
    assert_eq!(
        vec!["TestJobA", "TestJobB", "TestJobC"],
        executions.job_names(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_linear_dependency_skip_completed() {
    test_startup_job!(A, &[]);
    test_startup_job!(B, &["TestJobA"]);
    test_startup_job!(C, &["TestJobB"]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    run_startup_jobs_ex(
        &catalog,
        RunStartupJobsOptions::builder()
            .skip_completed_jobs(HashSet::from(["TestJobB"]))
            .build(),
    )
    .await
    .unwrap();

    // The order of executions must respect dependencies
    let executions = catalog.get_one::<JobExecutions>().unwrap();
    assert_eq!(vec!["TestJobA", "TestJobC"], executions.job_names(),);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_branching_dependency() {
    test_startup_job!(A, &[]);
    test_startup_job!(B, &["TestJobA"]);
    test_startup_job!(C, &["TestJobA"]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    run_startup_jobs(&catalog).await.unwrap();

    // Job A always runs first, while B & C may run in any order
    let executions = catalog.get_one::<JobExecutions>().unwrap();
    let actual_job_names = executions.job_names();
    assert_eq!("TestJobA", actual_job_names[0]);
    assert!(
        actual_job_names[1..] == ["TestJobB", "TestJobC"]
            || actual_job_names[1..] == ["TestJobC", "TestJobB"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_dependency() {
    test_startup_job!(B, &["TestJobA"]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobB>()
        .build();

    let res = run_startup_jobs(&catalog).await;
    assert_matches!(res, Err(StartupJobsError::DependsOnUnresolved(x))
        if x.job_name == "TestJobB" && x.unresolved_depends_on == "TestJobA"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dependency_loop() {
    test_startup_job!(A, &["TestJobC"]);
    test_startup_job!(B, &["TestJobA"]);
    test_startup_job!(C, &["TestJobB"]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA>()
        .add::<TestJobB>()
        .add::<TestJobC>()
        .build();

    let res = run_startup_jobs(&catalog).await;
    assert_matches!(res, Err(StartupJobsError::DependsOnLoop(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_unique_jobs() {
    test_startup_job!(A1, "A", &[]);
    test_startup_job!(A2, "A", &[]);

    let catalog = CatalogBuilder::new()
        .add::<JobExecutions>()
        .add::<TestJobA1>()
        .add::<TestJobA2>()
        .build();

    let res = run_startup_jobs(&catalog).await;
    assert_matches!(res, Err(StartupJobsError::JobNameNonUnique(x)) if x.job_name == "A");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

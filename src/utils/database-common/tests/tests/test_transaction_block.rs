// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use database_common::{DatabaseTransactionManager, TransactionRef, transaction};
use database_common_macros::{transactional_handler, transactional_method, transactional_method1};
use internal_error::{ErrorIntoInternal, InternalError};
use pretty_assertions::assert_eq;
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Metadata, Subscriber};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_value_block_commits_and_yields_its_value() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(harness.greeting_length(Step::Succeeds).await, Ok(5));
    assert_eq!(harness.transaction_endings(), ["COMMIT"]);
}

#[tokio::test]
async fn test_question_mark_rolls_back_and_leaves_the_function() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(
        harness.greeting_length(Step::Fails).await,
        Err("Internal error: step failed".to_string())
    );
    assert_eq!(harness.transaction_endings(), ["ROLLBACK"]);
}

#[tokio::test]
async fn test_result_block_commits_ok_and_rolls_back_err() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(harness.result_of_step(Step::Succeeds).await, Ok(()));
    assert_eq!(
        harness.result_of_step(Step::Fails).await,
        Err("Internal error: step failed".to_string())
    );
    assert_eq!(harness.result_of_step(Step::ReturnsEarly).await, Ok(()));
    assert_eq!(
        harness.transaction_endings(),
        ["COMMIT", "ROLLBACK", "COMMIT"]
    );
}

#[tokio::test]
async fn test_blocks_in_sequence_borrow_the_same_local() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(
        harness.steps_in_two_transactions().await,
        ["plan", "commit"]
    );
    assert_eq!(harness.transaction_endings(), ["COMMIT", "COMMIT"]);
}

#[tokio::test]
async fn test_catalog_binding_sees_the_transaction() {
    let harness = TransactionBlockHarness::new();

    assert!(harness.block_sees_transaction(true).await);
    assert!(!harness.block_sees_transaction(false).await);
    assert_eq!(harness.transaction_endings(), ["COMMIT"]);
}

#[tokio::test]
async fn test_transactional_method_keeps_attributes_and_ends_transaction() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(
        harness.method_sees_transaction(Step::Succeeds).await,
        Ok(true)
    );
    assert_eq!(
        harness.method_sees_transaction(Step::Fails).await,
        Err("Internal error: step failed".to_string())
    );
    assert_eq!(harness.transaction_endings(), ["COMMIT", "ROLLBACK"]);
    assert_eq!(
        harness.recorded_spans(),
        [
            "attribute_below_macro",
            "sees_transaction_in_method",
            "attribute_below_macro",
            "sees_transaction_in_method",
        ]
    );
}

#[tokio::test]
async fn test_transactional_method1_keeps_attributes_and_ends_transaction() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(harness.method_greeting_length(Step::Succeeds).await, Ok(5));
    assert_eq!(
        harness.method_greeting_length(Step::Fails).await,
        Err("Internal error: step failed".to_string())
    );
    assert_eq!(harness.transaction_endings(), ["COMMIT", "ROLLBACK"]);
    assert_eq!(
        harness.recorded_spans(),
        [
            "attribute_below_macro",
            "greeting_length_in_method",
            "attribute_below_macro",
            "greeting_length_in_method",
        ]
    );
}

#[tokio::test]
async fn test_transactional_handler_keeps_attributes_and_ends_transaction() {
    let harness = TransactionBlockHarness::new();

    assert_eq!(
        harness.handler_sees_transaction(Step::Succeeds).await,
        Ok(true)
    );
    assert_eq!(
        harness.handler_sees_transaction(Step::Fails).await,
        Err("Internal error: step failed".to_string())
    );
    assert_eq!(harness.transaction_endings(), ["COMMIT", "ROLLBACK"]);
    assert_eq!(
        harness.recorded_spans(),
        [
            "attribute_below_macro",
            "sees_transaction_in_handler",
            "attribute_below_macro",
            "sees_transaction_in_handler",
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
enum Step {
    Succeeds,
    Fails,
    ReturnsEarly,
}

impl Step {
    fn run(self) -> Result<(), InternalError> {
        match self {
            Self::Succeeds | Self::ReturnsEarly => Ok(()),
            Self::Fails => Err("step failed".int_err()),
        }
    }
}

struct Greeting(&'static str);

struct TransactionBlockHarness {
    catalog: dill::Catalog,
    transaction_manager: Arc<RecordingTransactionManager>,
    span_recorder: Arc<SpanRecorder>,
}

impl TransactionBlockHarness {
    fn new() -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add_value(Greeting("hello"));
        b.add_value(RecordingTransactionManager::default())
            .bind::<dyn DatabaseTransactionManager, RecordingTransactionManager>();
        let catalog = b.build();

        let transaction_manager = catalog.get_one().unwrap();
        Self {
            catalog,
            transaction_manager,
            span_recorder: Arc::default(),
        }
    }

    fn transaction_endings(&self) -> Vec<&'static str> {
        self.transaction_manager.endings.lock().unwrap().clone()
    }

    async fn greeting_length(&self, step: Step) -> Result<usize, String> {
        self.greeting_length_or_internal_error(step)
            .await
            .map_err(|e| e.reason())
    }

    async fn greeting_length_or_internal_error(&self, step: Step) -> Result<usize, InternalError> {
        let length = transaction!(|greeting: Arc<Greeting>| {
            step.run()?;
            greeting.0.len()
        });
        Ok(length)
    }

    async fn result_of_step(&self, step: Step) -> Result<(), String> {
        transaction!(|| -> Result<(), InternalError> {
            if let Step::ReturnsEarly = step {
                return Ok(());
            }
            step.run()
        })
        .map_err(|e| e.reason())
    }

    async fn steps_in_two_transactions(&self) -> Vec<&'static str> {
        self.steps_in_two_transactions_or_internal_error()
            .await
            .unwrap()
    }

    async fn steps_in_two_transactions_or_internal_error(
        &self,
    ) -> Result<Vec<&'static str>, InternalError> {
        let mut steps = Vec::new();
        transaction!(|| { steps.push("plan") });
        transaction!(|| { steps.push("commit") });
        Ok(steps)
    }

    async fn block_sees_transaction(&self, enabled: bool) -> bool {
        let base_catalog = self.catalog.clone();
        transaction!(
            #[catalog(base_catalog)]
            #[when(enabled)]
            |catalog| -> Result<_, InternalError> {
                Ok(catalog.get_one::<TransactionRef>().is_ok())
            }
        )
        .unwrap()
    }

    async fn method_sees_transaction(&self, step: Step) -> Result<bool, String> {
        self.recording_spans(self.sees_transaction_in_method(step))
            .await
            .map_err(|e| e.reason())
    }

    async fn method_greeting_length(&self, step: Step) -> Result<usize, String> {
        self.recording_spans(self.greeting_length_in_method(step))
            .await
            .map_err(|e| e.reason())
    }

    async fn handler_sees_transaction(&self, step: Step) -> Result<bool, String> {
        self.recording_spans(Self::sees_transaction_in_handler(
            Extension(self.catalog.clone()),
            step,
        ))
        .await
        .map_err(|e| e.reason())
    }

    async fn recording_spans<F: Future>(&self, future: F) -> F::Output {
        future
            .with_subscriber(Dispatch::new(Arc::clone(&self.span_recorder)))
            .await
    }

    fn recorded_spans(&self) -> Vec<&'static str> {
        self.span_recorder.names.lock().unwrap().clone()
    }

    // The span of each `#[tracing::instrument]` below is recorded only if the
    // macro above it keeps the attributes written below it

    #[transactional_method]
    #[tracing::instrument(name = "attribute_below_macro", skip_all)]
    async fn sees_transaction_in_method(&self, step: Step) -> Result<bool, InternalError> {
        step.run()?;
        Ok(transaction_catalog.get_one::<TransactionRef>().is_ok())
    }

    #[transactional_method1(greeting: Arc<Greeting>)]
    #[tracing::instrument(name = "attribute_below_macro", skip_all)]
    async fn greeting_length_in_method(&self, step: Step) -> Result<usize, InternalError> {
        step.run()?;
        Ok(greeting.0.len())
    }

    #[transactional_handler]
    #[tracing::instrument(name = "attribute_below_macro", skip_all)]
    async fn sees_transaction_in_handler(
        Extension(catalog): Extension<dill::Catalog>,
        step: Step,
    ) -> Result<bool, InternalError> {
        step.run()?;
        Ok(catalog.get_one::<TransactionRef>().is_ok())
    }
}

/// The shape of the request-catalog argument `#[transactional_handler]` looks for
struct Extension<T>(T);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records how each transaction ended. Ending one checks that nothing else
/// still refers to it
#[derive(Default)]
struct RecordingTransactionManager {
    endings: Mutex<Vec<&'static str>>,
}

impl RecordingTransactionManager {
    fn end(&self, transaction_ref: TransactionRef, ending: &'static str) {
        transaction_ref
            .downcast::<sqlx::Sqlite>()
            .into_inner_db_transaction();
        self.endings.lock().unwrap().push(ending);
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for RecordingTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        let never_connected_pool = sqlx::SqlitePool::connect_lazy("").unwrap();
        Ok(TransactionRef::new(never_connected_pool))
    }

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        self.end(transaction_ref, "COMMIT");
        Ok(())
    }

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        self.end(transaction_ref, "ROLLBACK");
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the name of each span opened while it is the default subscriber
#[derive(Default)]
struct SpanRecorder {
    names: Mutex<Vec<&'static str>>,
}

impl Subscriber for SpanRecorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut names = self.names.lock().unwrap();
        names.push(span.metadata().name());
        Id::from_u64(u64::try_from(names.len()).unwrap())
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{EmptySubscription, Object, value};
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use kamu_adapter_graphql::*;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn logged_in_guard_logged() {
    let catalog = dill::CatalogBuilder::new()
        .add_value(CurrentAccountSubject::new_test())
        .build();

    let schema = TestSchema::build(TestQuery, TestMutation, EmptySubscription)
        .data(catalog)
        .finish();

    let query_response = schema
        .execute(
            r#"
            query {
                guardedQuery,
                unguardedQuery,
            }
            "#,
        )
        .await;

    assert!(query_response.is_ok(), "{query_response:?}");
    assert_eq!(
        query_response.data,
        value!({
            "guardedQuery": 1,
            "unguardedQuery": 2,
        })
    );

    let mutation_response = schema
        .execute(
            r#"
            mutation {
                guardedMutation,
                unguardedMutation,
            }
            "#,
        )
        .await;

    assert!(mutation_response.is_ok(), "{mutation_response:?}");
    assert_eq!(
        mutation_response.data,
        value!({
            "guardedMutation": 3,
            "unguardedMutation": 4,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn logged_in_guard_anonymous() {
    let catalog = dill::CatalogBuilder::new()
        .add_value(CurrentAccountSubject::anonymous(
            AnonymousAccountReason::NoAuthenticationProvided,
        ))
        .build();

    let schema = TestSchema::build(TestQuery, TestMutation, EmptySubscription)
        .data(catalog)
        .finish();

    let guarded_query_response = schema
        .execute(
            r#"
            query {
                guardedQuery,
            }
            "#,
        )
        .await;

    let unguarded_query_response = schema
        .execute(
            r#"
            query {
                unguardedQuery,
            }
            "#,
        )
        .await;

    assert_eq!(guarded_query_response.errors.len(), 1);
    assert_eq!(
        guarded_query_response.errors[0].message,
        ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE
    );

    assert!(
        unguarded_query_response.is_ok(),
        "{unguarded_query_response:?}"
    );
    assert_eq!(
        unguarded_query_response.data,
        value!({
            "unguardedQuery": 2,
        })
    );

    let guarded_mutation_response = schema
        .execute(
            r#"
            mutation {
                guardedMutation,
            }
            "#,
        )
        .await;

    let unguarded_mutation_response = schema
        .execute(
            r#"
            mutation {
                unguardedMutation,
            }
            "#,
        )
        .await;

    assert_eq!(guarded_mutation_response.errors.len(), 1);
    assert_eq!(
        guarded_mutation_response.errors[0].message,
        ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE
    );

    assert!(
        unguarded_mutation_response.is_ok(),
        "{unguarded_mutation_response:?}"
    );
    assert_eq!(
        unguarded_mutation_response.data,
        value!({
            "unguardedMutation": 4,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestQuery;

struct TestMutation;

#[Object]
impl TestQuery {
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn guarded_query(&self) -> u32 {
        1
    }

    async fn unguarded_query(&self) -> u32 {
        2
    }
}

#[Object]
impl TestMutation {
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn guarded_mutation(&self) -> u32 {
        3
    }

    async fn unguarded_mutation(&self) -> u32 {
        4
    }
}

type TestSchema = async_graphql::Schema<TestQuery, TestMutation, EmptySubscription>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

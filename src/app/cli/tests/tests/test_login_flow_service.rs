// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::assert_matches;
use std::sync::{Arc, Mutex};

use internal_error::InternalError;
use kamu::domain::{
    AddRepoError,
    DeleteRepoError,
    GetRepoError,
    RemoteRepositoryRegistry,
    RepositoryAccessInfo,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_cli::odf_server::*;
use kamu_cli::resource_context::ResourceContextStoreScope;
use kamu_cli::{ContextAddCommand, OutputConfig};
use pretty_assertions::assert_eq;
use time_source::SystemTimeSourceDefault;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_SERVER_URL: &str = "http://api.example.com/";
const TEST_ACCESS_TOKEN: &str = "test-access-token";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// should_auto_login: the TTY gate that decides whether a browser may open
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_should_auto_login_only_in_tty() {
    // The headline case: a plain `context add` in a real terminal logs in
    assert!(ContextAddCommand::should_auto_login(false, true, false));

    // Non-TTY (CI, scripts, e2e) must never block on a browser
    assert!(!ContextAddCommand::should_auto_login(false, false, false));
}

#[test]
fn test_should_auto_login_suppressed_by_no_login() {
    assert!(!ContextAddCommand::should_auto_login(true, true, false));
    assert!(!ContextAddCommand::should_auto_login(true, true, true));
}

#[test]
fn test_should_auto_login_e2e_backend_override_bypasses_tty_check() {
    // The e2e harness is not a TTY, so the hidden override is the only way to
    // exercise the interactive path end-to-end
    assert!(ContextAddCommand::should_auto_login(false, false, true));

    // ...but it never overrides an explicit opt-out
    assert!(!ContextAddCommand::should_auto_login(true, false, true));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scope mapping
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_resource_context_scope_maps_to_access_token_scope() {
    assert_matches!(
        AccessTokenStoreScope::from(ResourceContextStoreScope::Workspace),
        AccessTokenStoreScope::Workspace
    );
    assert_matches!(
        AccessTokenStoreScope::from(ResourceContextStoreScope::User),
        AccessTokenStoreScope::User
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExistingToken: the one login method that performs no network calls
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_existing_token_is_saved_under_both_urls() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    let outcome = harness
        .ensure_logged_in(AccessTokenStoreScope::Workspace, true)
        .await
        .unwrap();

    assert_matches!(outcome.result, LoginFlowResult::LoggedIn);
    assert_eq!(outcome.backend_url.as_str(), TEST_SERVER_URL);

    // The URL acts as both frontend and backend, so either lookup must find it
    let by_frontend = harness
        .registry
        .find_by_frontend_url(&test_server_url())
        .unwrap();
    assert_eq!(by_frontend.access_token.access_token, TEST_ACCESS_TOKEN);

    let by_backend = harness
        .registry
        .find_by_backend_url(&test_server_url())
        .unwrap();
    assert_eq!(by_backend.access_token.access_token, TEST_ACCESS_TOKEN);
}

#[test_log::test(tokio::test)]
async fn test_existing_token_adds_repository_in_workspace_scope() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    harness
        .ensure_logged_in(AccessTokenStoreScope::Workspace, true)
        .await
        .unwrap();

    assert_eq!(
        harness.repo_reg.added_repos(),
        vec![(
            "api.example.com".to_string(),
            "odf+http://api.example.com/".to_string()
        )]
    );
}

#[test_log::test(tokio::test)]
async fn test_existing_token_skips_repository_in_user_scope() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    harness
        .ensure_logged_in(AccessTokenStoreScope::User, true)
        .await
        .unwrap();

    assert!(harness.repo_reg.added_repos().is_empty());
}

#[test_log::test(tokio::test)]
async fn test_existing_token_skips_repository_when_add_repo_is_off() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    harness
        .ensure_logged_in(AccessTokenStoreScope::Workspace, false)
        .await
        .unwrap();

    assert!(harness.repo_reg.added_repos().is_empty());
}

/// The interactive/existing-token derivation names the repository after the
/// server host; the silent one derives it from the backend host instead.
#[test_log::test(tokio::test)]
async fn test_existing_token_repo_name_defaults_to_host() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    harness
        .ensure_logged_in_with(
            AccessTokenStoreScope::Workspace,
            true,
            None,
            &Url::parse("http://some-host.example.com:8080/").unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        harness.repo_reg.added_repos(),
        vec![(
            // The host alone, without the port
            "some-host.example.com".to_string(),
            "odf+http://some-host.example.com:8080/".to_string()
        )]
    );
}

#[test_log::test(tokio::test)]
async fn test_existing_token_explicit_repo_name_wins() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    harness
        .ensure_logged_in_with(
            AccessTokenStoreScope::Workspace,
            true,
            Some(odf::RepoName::new_unchecked("my-repo")),
            &test_server_url(),
        )
        .await
        .unwrap();

    assert_eq!(
        harness.repo_reg.added_repos(),
        vec![(
            "my-repo".to_string(),
            "odf+http://api.example.com/".to_string()
        )]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Frontend/backend URL divergence
//
// A backend-keyed login method given a frontend URL stores the token where the
// context can never look it up.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_only_interactive_login_targets_the_typed_url() {
    let typed = Url::parse("http://platform.example.com/").unwrap();
    let resolved = Url::parse("http://api.example.com/").unwrap();

    // The browser flow discovers the backend itself, so it needs the typed URL
    assert_eq!(
        ContextAddCommand::login_server_url(
            &LoginMethod::Interactive {
                predefined_backend_url: None
            },
            &typed,
            &resolved,
        ),
        typed
    );

    // The rest key their token by the URL they are given, and the context looks
    // that token up by the resolved backend URL
    for method in [
        LoginMethod::ExistingToken {
            access_token: TEST_ACCESS_TOKEN.to_string(),
        },
        LoginMethod::OAuth {
            provider: "github".to_string(),
            access_token: TEST_ACCESS_TOKEN.to_string(),
        },
        LoginMethod::Password {
            login: "user".to_string(),
            password: "password".to_string(),
        },
    ] {
        assert_eq!(
            ContextAddCommand::login_server_url(&method, &typed, &resolved),
            resolved,
            "{method:?} must target the resolved backend URL"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_existing_token_is_keyed_by_the_url_it_was_given() {
    let harness = Harness::new(CurrentAccountSubject::new_test());

    // Stand in for what `context add` passes once it has resolved the backend
    let backend_url = Url::parse("http://api.example.com/").unwrap();
    let frontend_url = Url::parse("http://platform.example.com/").unwrap();

    harness
        .ensure_logged_in_with(AccessTokenStoreScope::Workspace, false, None, &backend_url)
        .await
        .unwrap();

    // The context looks its token up by backend URL, so it must be found there
    assert!(
        harness.registry.find_by_backend_url(&backend_url).is_some(),
        "token must be retrievable by the backend URL the context stores"
    );

    // ...and must NOT have landed under an unrelated frontend URL
    assert!(
        harness
            .registry
            .find_by_frontend_url(&frontend_url)
            .is_none(),
        "token must not be keyed by a URL that was never passed in"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A record matching the URL but holding no token for this account must not
/// stop the search: `context add --user` drops its workspace token and saves a
/// user one, and the emptied workspace record would otherwise mask it.
#[test_log::test(tokio::test)]
async fn test_tokenless_workspace_record_does_not_mask_a_user_token() {
    let harness = Harness::new(CurrentAccountSubject::new_test());
    let backend_url = Url::parse("http://api.example.com/").unwrap();

    harness
        .registry
        .save_access_token(
            AccessTokenStoreScope::Workspace,
            None,
            &backend_url,
            "workspace-token".to_string(),
        )
        .unwrap();
    harness
        .registry
        .drop_access_token(AccessTokenStoreScope::Workspace, &backend_url)
        .unwrap();

    harness
        .registry
        .save_access_token(
            AccessTokenStoreScope::User,
            None,
            &backend_url,
            "user-token".to_string(),
        )
        .unwrap();

    let report = harness
        .registry
        .find_by_backend_url(&backend_url)
        .expect("the user token must be found past the emptied workspace record");

    assert_eq!(report.access_token.access_token, "user-token");
    assert_matches!(report.scope, AccessTokenStoreScope::User);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lookups span both stores, so a token's scope need not be the requested one.
/// Dropping from the requested scope instead silently leaves it in place.
#[test_log::test(tokio::test)]
async fn test_find_report_carries_the_store_the_token_was_found_in() {
    let harness = Harness::new(CurrentAccountSubject::new_test());
    let backend_url = Url::parse("http://api.example.com/").unwrap();

    // Only the user store holds a token, so a report claiming the requested
    // scope (or a hardcoded workspace) would be wrong
    harness
        .registry
        .save_access_token(
            AccessTokenStoreScope::User,
            None,
            &backend_url,
            TEST_ACCESS_TOKEN.to_string(),
        )
        .unwrap();

    let report = harness
        .registry
        .find_by_backend_url(&backend_url)
        .expect("token should be found by backend URL");

    assert_matches!(report.scope, AccessTokenStoreScope::User);

    // What a workspace-scoped invocation would do if it trusted its own scope
    let dropped = harness
        .registry
        .drop_access_token(AccessTokenStoreScope::Workspace, &report.backend_url)
        .unwrap();

    assert!(!dropped, "the token does not live in the workspace store");
    assert!(
        harness.registry.find_by_backend_url(&backend_url).is_some(),
        "the token is still there, ready to fail again on the next retry"
    );

    let dropped = harness
        .registry
        .drop_access_token(report.scope, &report.backend_url)
        .unwrap();

    assert!(dropped, "a drop scoped to the found store must remove it");
    assert!(
        harness.registry.find_by_backend_url(&backend_url).is_none(),
        "token must be gone after the drop"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A token saved with no frontend URL is matched only by its backend URL.
/// Registry-level: the service's own branch needs a backend returning 400,
/// which `LoginService`'s inline `reqwest` client cannot be faked into.
#[test_log::test(tokio::test)]
async fn test_frontend_keyed_drop_misses_a_backend_only_token() {
    let harness = Harness::new(CurrentAccountSubject::new_test());
    let backend_url = Url::parse("http://api.example.com/").unwrap();

    // Save the way a silent login does: backend URL only, no frontend
    harness
        .registry
        .save_access_token(
            AccessTokenStoreScope::Workspace,
            None,
            &backend_url,
            TEST_ACCESS_TOKEN.to_string(),
        )
        .unwrap();

    let report = harness
        .registry
        .find_by_backend_url(&backend_url)
        .expect("token should be found by backend URL");
    assert!(
        report.frontend_url.is_none(),
        "this test is only meaningful for a token with no frontend URL"
    );

    // Keying the drop off a frontend URL silently does nothing — this is the
    // shape of the bug, and why the service must not pass the typed URL here
    let frontend_url = Url::parse("http://platform.example.com/").unwrap();
    let dropped = harness
        .registry
        .drop_access_token(AccessTokenStoreScope::Workspace, &frontend_url)
        .unwrap();

    assert!(!dropped, "a frontend-keyed drop cannot match this token");
    assert!(
        harness.registry.find_by_backend_url(&backend_url).is_some(),
        "the token is still there, ready to fail again on the next retry"
    );

    let dropped = harness
        .registry
        .drop_access_token(AccessTokenStoreScope::Workspace, &report.backend_url)
        .unwrap();

    assert!(dropped, "a backend-keyed drop must remove the token");
    assert!(
        harness.registry.find_by_backend_url(&backend_url).is_none(),
        "token must be gone after the drop"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_subject_is_rejected_rather_than_panicking() {
    let harness = Harness::new(CurrentAccountSubject::anonymous(
        kamu_accounts::AnonymousAccountReason::NoAuthenticationProvided,
    ));

    let res = harness
        .ensure_logged_in(AccessTokenStoreScope::Workspace, true)
        .await;

    assert_matches!(res, Err(e) if e.to_string().contains("anonymous session"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_server_url() -> Url {
    Url::parse(TEST_SERVER_URL).unwrap()
}

struct Harness {
    login_flow_service: LoginFlowService,
    registry: Arc<AccessTokenRegistryService>,
    repo_reg: Arc<RecordingRepositoryRegistry>,
}

impl Harness {
    fn new(subject: CurrentAccountSubject) -> Self {
        let registry = Arc::new(AccessTokenRegistryService::new(
            Arc::new(InMemoryAccessTokenStore::new()),
            Arc::new(subject.clone()),
        ));
        let repo_reg = Arc::new(RecordingRepositoryRegistry::new());

        let login_flow_service = LoginFlowService::new(
            Arc::new(LoginService::new(
                Arc::new(SystemTimeSourceDefault),
                /* is_e2e_testing */ true,
            )),
            registry.clone(),
            repo_reg.clone(),
            Arc::new(subject),
            Arc::new(OutputConfig::default()),
        );

        Self {
            login_flow_service,
            registry,
            repo_reg,
        }
    }

    async fn ensure_logged_in(
        &self,
        scope: AccessTokenStoreScope,
        add_repo: bool,
    ) -> Result<LoginFlowOutcome, kamu_cli::CLIError> {
        self.ensure_logged_in_with(scope, add_repo, None, &test_server_url())
            .await
    }

    async fn ensure_logged_in_with(
        &self,
        scope: AccessTokenStoreScope,
        add_repo: bool,
        repo_name: Option<odf::RepoName>,
        server_url: &Url,
    ) -> Result<LoginFlowOutcome, kamu_cli::CLIError> {
        self.login_flow_service
            .ensure_logged_in(LoginFlowOptions {
                server_url: server_url.clone(),
                scope,
                method: LoginMethod::ExistingToken {
                    access_token: TEST_ACCESS_TOKEN.to_string(),
                },
                add_repo,
                repo_name,
                report_progress: false,
            })
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Retains what was written, so saves can be read back.
struct InMemoryAccessTokenStore {
    workspace: Mutex<OdfServerAccessTokenRegistry>,
    user: Mutex<OdfServerAccessTokenRegistry>,
}

impl InMemoryAccessTokenStore {
    fn new() -> Self {
        Self {
            workspace: Mutex::new(Vec::new()),
            user: Mutex::new(Vec::new()),
        }
    }

    fn slot(&self, scope: AccessTokenStoreScope) -> &Mutex<OdfServerAccessTokenRegistry> {
        match scope {
            AccessTokenStoreScope::Workspace => &self.workspace,
            AccessTokenStoreScope::User => &self.user,
        }
    }
}

impl AccessTokenStore for InMemoryAccessTokenStore {
    fn read_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
    ) -> Result<OdfServerAccessTokenRegistry, InternalError> {
        Ok(self.slot(scope).lock().unwrap().clone())
    }

    fn write_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
        registry: &OdfServerAccessTokenRegistry,
    ) -> Result<(), InternalError> {
        (*self.slot(scope).lock().unwrap()).clone_from(registry);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records `add_repository` calls so they can be asserted on.
struct RecordingRepositoryRegistry {
    added: Mutex<Vec<(String, String)>>,
}

impl RecordingRepositoryRegistry {
    fn new() -> Self {
        Self {
            added: Mutex::new(Vec::new()),
        }
    }

    fn added_repos(&self) -> Vec<(String, String)> {
        self.added.lock().unwrap().clone()
    }
}

impl RemoteRepositoryRegistry for RecordingRepositoryRegistry {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = odf::RepoName> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(
        &self,
        repo_name: &odf::RepoName,
    ) -> Result<RepositoryAccessInfo, GetRepoError> {
        Err(kamu::domain::RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }

    fn add_repository(&self, repo_name: &odf::RepoName, url: Url) -> Result<(), AddRepoError> {
        self.added
            .lock()
            .unwrap()
            .push((repo_name.to_string(), url.to_string()));
        Ok(())
    }

    fn delete_repository(&self, repo_name: &odf::RepoName) -> Result<(), DeleteRepoError> {
        Err(kamu::domain::RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

---
name: kamu-cli-e2e-tests
description: >
    CLI end-to-end (black-box) test machinery for the Kamu CLI under
    src/e2e/app/cli. Use this skill whenever writing, wiring, or reviewing CLI
    e2e tests that drive the `kamu` binary as a subprocess (or its API server)
    and assert on observable behavior — stdout/stderr/exit code or API
    responses. Covers: where shared scenario bodies live, the two harness modes
    (execute_command vs run_api_server) and their fixture signatures, the
    per-database wiring macros and SQLite/Postgres/MySQL mirror files, the
    combined CLI↔server (login/remote) pattern, generating permutation wrapper
    fns with `paste!`, and the KamuCliPuppet / KamuApiServerClient APIs. This is
    distinct from `kamu-test-harness`, which covers in-process dill
    unit/integration harnesses, not subprocess CLI tests.
---

# Kamu CLI E2E Tests

See also: `kamu-test-harness` for in-process (non-subprocess) test harnesses.

CLI e2e tests live under `src/e2e/app/cli/`. They exercise the real `kamu`
binary — either as a subprocess asserting on stdout/stderr/exit code, or by
booting its API server and driving it through an HTTP/GraphQL client. They are
**expensive**, so prefer lower-level coverage where it exists and reserve e2e
for user-visible contracts that are hard to validate below the CLI/API layer.

This skill is command-agnostic: the same framework backs dataset commands
(`add`, `pull`, `push`, `delete`, `ingest`, …), system commands, REST/GraphQL
API scenarios, and any newer command families. Command-specific details belong
in the test itself, not in this skill.

---

## Crate layout

```
src/e2e/app/cli/
  repo-tests/        # SHARED scenario bodies (the actual test logic)
    src/
      commands/      # one file per command area; mod.rs re-exports all
      rest_api/      # API-server-mode scenarios
      lib.rs
  common/            # harness + KamuApiServerClient + prelude
    src/e2e_harness.rs
  common-macros/     # the per-DB instantiation macros
    lib.rs
  sqlite/  postgres/  mysql/   # per-DB WIRING (thin macro invocations)
    tests/tests/commands/...
```

Key idea: **scenario logic is written once** in `repo-tests`, then **instantiated
per database** by thin wiring files in `sqlite/`, `postgres/`, `mysql/`.

---

## Two harness modes (pick the right fixture signature)

The harness is `KamuCliApiServerHarness` in
`common/src/e2e_harness.rs`. It has two entry methods, and the fixture function's
parameter type tells you which one runs:

| Harness method     | Macro                                  | Fixture signature                        | What you get |
|--------------------|----------------------------------------|------------------------------------------|--------------|
| `execute_command`  | `kamu_cli_execute_command_e2e_test!`   | `async fn(kamu: KamuCliPuppet)`          | A CLI workspace puppet, **no server** |
| `run_api_server`   | `kamu_cli_run_api_server_e2e_test!`    | `async fn(client: KamuApiServerClient)`  | A **running API server** client, no co-located puppet |

There is **no single harness that gives both** a server and a CLI puppet wired
to it. To drive the CLI against a server (remote context, push/pull, login), use
the combined pattern below.

`KamuCliApiServerHarnessOptions` builder: `.with_multi_tenant()`,
`.with_kamu_config(<&str>)`, `.with_no_workspace()`,
`.with_today_as_frozen_system_time()`, `.with_custom_frozen_system_time(dt)`.

---

## Writing a shared scenario body

Put it in `repo-tests/src/commands/test_<area>.rs` as a `pub async fn`, then
register it in `repo-tests/src/commands/mod.rs` (both `mod test_<area>;` and
`pub use test_<area>::*;`).

```rust
pub async fn test_delete_dataset(kamu: KamuCliPuppet) {
    kamu.assert_failure_command_execution(
        ["delete", "player-scores"],
        None,
        Some(["Error: Dataset not found: player-scores"]),
    ).await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await.success();

    kamu.assert_success_command_execution(
        ["--yes", "delete", "player-scores"],
        None,
        Some([r#"Deleted: player-scores"#]),
    ).await;
}
```

### KamuCliPuppet API you will use most

`src/utils/kamu-cli-puppet/src/kamu_cli_puppet.rs` (+ `_ext.rs`):

- `kamu.execute(args).await.success()` / `.failure()` — run + assert exit only.
- `kamu.execute_with_input(args, stdin).await` — feed stdin (e.g. `--stdin` input).
- `kamu.assert_success_command_execution(args, stdin_opt, Some([regex, …]))` and
  the `_failure_` variant — assert exit status **and** that each regex matches a
  line of stdout/stderr. **Patterns are regexes** — escape `(`, `)`, `.`, etc.
  (the codebase uses `r#"…\(s\)"#` and `regex::escape` for dynamic strings).
- `set_account(Some(name))`, `create_account(&name)` for multi-tenant scenarios.
- `KamuCliPuppet::new_workspace_tmp(is_multi_tenant)` / `_single_tenant()` /
  `_multi_tenant()` — create a fresh standalone workspace (used in the combined pattern).
- `workspace_path()`, `set_system_time(...)`.

Assertion philosophy: prefer behavioral regex on stdout; **avoid** asserting
volatile UUIDs/timestamps or exact table layout. Use `pretty_assertions::assert_eq!`
when comparing parsed/structured values.

---

## Per-database wiring (the macros)

Defined in `common-macros/lib.rs`. Each wiring file invokes one macro **per test**.
The macro expands to a `#[test]` tagged with `#[test_group::group(e2e, …)]`; the
generated test fn name is the last path segment of `fixture`.

`sqlite/tests/tests/commands/test_<area>.rs`:

```rust
use kamu_cli_e2e_common::prelude::*;

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_delete_dataset
);

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_delete_dataset_all_respects_current_account,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(MULTITENANT_KAMU_CONFIG_WITH_DEFAULT_USER),
    extra_test_groups = "engine, ingest, datafusion"
);
```

`postgres/tests/tests/commands/test_<area>.rs` is the **identical file with
`storage = postgres`** (Postgres/MySQL macros add a `sqlx::test(migrator = …)`
pool arg; SQLite creates its own DB).

> **Lockstep rule:** every fixture must be listed in *each* DB directory you want
> it to run on. Adding a test to `sqlite/` but not `postgres/` silently drops
> Postgres coverage. Keep the mirror files in sync.

Use `run_api_server` form for server-backed fixtures:

```rust
kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_delete_warning
    options = Options::default().with_multi_tenant().with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);
```

---

## The local↔remote combined pattern (driving CLI against a server)

When a scenario needs the CLI to talk to a running server (login, push/pull, or
any command that targets a remote node), write a `run_api_server` fixture and,
inside it, spin a **separate** CLI workspace and authenticate. Precedent:
`repo-tests/src/test_smart_transfer_protocol.rs`.

```rust
pub async fn test_xxx(mut client: KamuApiServerClient) {
    // 1. Get a token from the running server
    let token = client.auth().login_as_e2e_user().await;     // -> AccessToken

    // 2. A separate CLI workspace puppet
    let kamu = KamuCliPuppet::new_workspace_tmp(/* multi_tenant */ true).await;

    // 3. Authenticate it against the server
    kamu.execute([
        "login", client.get_base_url().as_str(), "--access-token", token.as_str(),
    ]).await.success();

    // 4. Drive CLI commands that hit the server
    // ...
}
```

`KamuApiServerClient` (`common/src/kamu_api_server_client.rs` + `_ext.rs`):
`get_base_url()`, `get_odf_node_url()`, `auth().login_as_e2e_user()`,
`dataset().get_odf_endpoint(&alias)`.

---

## One scenario body, many permutations (`paste!` wrappers)

When the *same* scenario must run under several configurations (single- vs
multi-tenant, local subprocess vs remote-server, different input formats, …),
write the scenario body **once** taking the varying inputs as parameters, then
generate the per-permutation `pub async fn` wrappers with `paste::paste!`. Each
generated wrapper has a fixture-compatible signature so it can be wired
individually. Precedent at the top of
`repo-tests/src/test_smart_transfer_protocol.rs`:

```rust
macro_rules! test_smart_transfer_protocol_permutations {
    ($test_name: expr) => {
        paste::paste! {
            pub async fn [<$test_name "_st_st">](client: KamuApiServerClient) {
                $test_name(client, false, false).await;
            }
            pub async fn [<$test_name "_st_mt">](client: KamuApiServerClient) {
                $test_name(client, false, true).await;
            }
            // ...mt_st, mt_mt
        }
    };
}

test_smart_transfer_protocol_permutations!(test_smart_push_smart_pull_sequence);
```

Each generated name (`…_st_st`, `…_st_mt`, …) is then wired separately in the
per-DB files.

### Generating local/remote permutation pairs from one scenario fn

A common need is one scenario body that must run both as a local subprocess and
against a remote server. The boilerplate-free approach is a **dedicated proc
macro** that, from a single `fixture = <scenario fn>` + one wiring line per
storage, emits two separately-runnable test fns — a `_local` one wired to the
`execute_command` harness and a `_remote` one wired to the `run_api_server`
harness — each constructing whatever "where do commands run" abstraction the
scenario takes. This collapses both the hand-written wrapper fns and the
duplicated per-DB macro invocations.

When a feature area needs this, add a thin proc macro next to the existing ones
in `common-macros/src/lib.rs` (reuse the shared `InputArgs` parser; derive the
`_local`/`_remote` fn names with `format_ident!`) and re-export it from the
prelude. The generated bodies should reference the scenario's helper type by its
fully-qualified path so wiring files need no extra `use`.

---

## Running the tests

Always export `SQLX_OFFLINE=true` for any cargo build/test/clippy (Postgres
crates need the offline SQLx cache):

```bash
SQLX_OFFLINE=true cargo nextest run -E 'test(test_my_command_)'  # filter by name
SQLX_OFFLINE=true cargo build -p kamu-cli-e2e-repo-tests          # compile shared bodies
SQLX_OFFLINE=true make clippy
```

SQLite e2e tests run without a DB container. Postgres/MySQL tests are tagged
`#[test_group::group(e2e, database, postgres|mysql, …)]` and require the
corresponding DB available per repo convention.

---

## Checklist for adding a CLI e2e test

1. Write the scenario body in `repo-tests/src/commands/test_<area>.rs` (or
   `repo-tests/src/rest_api/` for API-server-mode scenarios).
2. Register it in `commands/mod.rs` (`mod` + `pub use`).
3. Add wiring in `sqlite/.../test_<area>.rs` **and** `postgres/.../test_<area>.rs`
   (lockstep) with the correct macro for its fixture signature.
4. For server-backed/remote scenarios, use `run_api_server` + the combined login
   pattern.
5. Build the repo-tests crate, then run the SQLite permutation green before
   handing off. Keep assertions regex-based and free of volatile values.

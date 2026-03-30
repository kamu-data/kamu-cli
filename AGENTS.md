# AGENTS.md

Small project-specific guidance for coding agents working in this repository.

## Validation

- Run `cargo fmt` after edits.
- Run `make clippy` before considering the task finished.
- Treat Clippy warnings as errors to fix, not to ignore.


## Tests

- Prefer `cargo nextest run` over `cargo test` for targeted test execution.
- Prefer workspace-level incremental builds with test filters instead of narrowing by package when possible.
- Typical pattern:

```bash
cargo nextest run -E 'test(test_name_here)'
```


## Postgres / sqlx

- Do not assume Postgres is unavailable.
- This repo relies local Dockerized Postgres instance for `sqlx` macro validation and related checks.
- Similarly, there is a local SQLite database created for similar purposes.
- In Postgres/SQLite repositories, prefer `sqlx::query!`, `sqlx::query_as!`, and similar macros over function-based queries for compile-time SQL checking.
- When adding new DB-backed infra crates to the Makefile SQLx crate lists, or when adding new migrations, run `make sqlx-local-setup` to recreate the local databases, rerun migrations, and refresh `.env` bindings for all affected crates.
- When modifying SQLx queries, run `make sqlx-prepare`.
- `make lint` includes SQLx cache validation via `make lint-sqlx`.
- If a sandbox blocks DB access, rerun the relevant command with the needed permissions rather than changing the workflow.


## Style

- Follow existing Rust style and naming in surrounding code.
- Prefer inline formatting like `format!("value={value}")`.
- Prefer checked numeric conversions like `usize::try_from(x).unwrap()` when narrowing types.
- Respect exact long separator comment style where surrounding files use it.
- Name lookup methods consistently: `get_xxx` returns the thing or a not-found error, while `find_xxx` returns `Option<T>` and treats absence as non-error.


## Design Notes

### Outbox

- For outbox events that leave the bounded context, prefer snapshot-style payloads over incremental deltas.
- In such resource change detection flows, optimize detection around the emission gate 
(`did effective state change?`), then re-query current state for the outgoing message.
- In tests with `MockOutbox`, prefer hiding message expectation setup in harness/helper methods instead of inline closures in each test.

### Repositories
- Keep repository layers simple when possible; prefer domain/service-level complex algorithms rather than repositories unless storage-specific behavior is the actual concern.

### Error Handling
- Prefer operation-specific error types and keep impossible variants out of operation-specific APIs when practical.
- When translation is structural only, implement `From` near the error type definition and keep service/use-case code at a higher level.
- Reuse shared domain error types for repeated concepts such as resource not found, type mismatch, and load failures.
- Do not use catch-all match arms for error conversion; list variants explicitly.
- Prefer plain `InternalError` for generic infrastructure/read failures; only introduce named wrappers when the distinction is meaningful at the boundary.
- Prefer `.int_err()` plus `.with_context(...)` over ad hoc `InternalError::new(format!(...))` when converting errors.


## Scope

- Keep new guidance here short and repo-specific.
- Use `.github/copilot-instructions.md` as supporting context, but prefer this file for the highest-signal local workflow notes.

# GitHub Copilot Instructions for Kamu CLI Project

## Code Quality and Linting

This project uses Rust with Clippy for linting and code quality checks. When performing any code analysis, validation, or after making code changes, you should:

1. **Always run formatting first** using `cargo fmt` before running any linting checks
2. **Always run Clippy** using `make clippy` to check for linting issues
3. Run Clippy before and after making code changes to ensure no new warnings are introduced
4. If Clippy reports warnings or errors, address them as part of the code changes
5. Use `cargo check` for basic compilation checks when needed
6. The project follows strict linting standards - treat Clippy warnings as errors that need to be fixed

## Code Style
- Follow the project's existing code style and formatting
- The project uses rustfmt for code formatting
- Respect the existing module structure and naming conventions
- **Use inline format syntax**: Prefer `format!("Message: {variable}")` over `format!("Message: {}", variable)` for better readability and Clippy compliance
- **Use safe numeric casting**: Prefer `usize::try_from(x).unwrap()` over `x as usize` for potentially unsafe numeric casts to avoid Clippy warnings

## Testing
- This project uses `cargo nextest` for running tests
- Run tests with `cargo nextest run` when making changes that could affect functionality
- Ensure all tests pass before considering changes complete
- Tests should never refer internal methods or fields directly; use public interfaces for testing

## Event Modeling Style
- Prefer using enums for event types to ensure type safety and clarity.
- Use descriptive names for events to convey their purpose clearly.
- Implement serialization and deserialization traits for all event enums to facilitate data interchange.
- Keep event structures simple and focused on the data they need to carry.
- Document each event type with comments explaining its usage and context.
- Prefer using separate structs for event data rather than inline data in the enum to enhance clarity and maintainability.

## Changelog

When completing a full PR (not incremental local changes), update the `CHANGELOG.md` file:
- Add entries to the `[Unreleased]` section at the top of the file
- Use the appropriate subsection: `### Added` for new features, `### Changed` for modifications, `### Fixed` for bug fixes
- Write a brief, high-level summary of the change from an end-user perspective
- This only applies to full PRs, not to incremental work done via local IDE Copilot

Remember: **Always run `cargo fmt` followed by `make clippy` as part of your validation process when working with this codebase.**

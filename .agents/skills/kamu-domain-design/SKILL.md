---
name: kamu-domain-design
description: Domain modeling, outbox, repository, view construction, event modeling, and error-handling guidance for Kamu CLI. Use when designing domain services, resource views, outbox events, domain errors, event payloads, or repository/service responsibility boundaries.
---

# Kamu Domain Design

Use this skill for domain-level decisions that are not specific to a storage engine or API adapter.

## Outbox

- For outbox events that leave the bounded context, prefer snapshot-style payloads over incremental deltas.
- In resource change detection flows, optimize around the emission gate: did effective state change?
- After deciding to emit, re-query current state for the outgoing message.
- In tests with `MockOutbox`, hide message expectation setup in harness/helper methods instead of inline closures in each test.

## Bounded Contexts and Foreign Keys

### 1. Cross-Context Foreign Key Rules

- Never use database foreign keys to reference tables that belong to a different bounded context (e.g., `dataset_entries` from the datasets domain referenced by a configuration or flow table). Such FKs create hard schema coupling that blocks future service splits.
- Foreign keys that stay within the same bounded context (e.g., configuration projections referencing `resources`) are fine and should be kept.

### 2. Asynchronous Cleanup Pattern

- For cross-context references, implement asynchronous cleanup instead of foreign keys.
- Subscribe to the relevant lifecycle outbox message (e.g., `DatasetLifecycleMessage::Deleted`) from a `MessageConsumerT` in the dependent bounded context's services layer.
- Perform the cleanup in a new transaction.

### 3. Cleanup Failure Handling

- If the cleanup transaction fails, log the error with full context.
- The message consumer framework will retry the message using its configured retry strategy.
- For persistent failures, ensure dead-letter handling is configured to capture unprocessable messages.

### 4. Examples

- See `ConfigurationDatasetLifecycleMessageConsumer` and `FlowDatasetsEventBridge` as examples of the cross-context async deletion pattern.

## Repositories

- Keep repositories simple when possible.
- Prefer domain/service-level complex algorithms over repository-level algorithms unless the algorithm directly depends on storage-specific features (e.g., database-specific indexing, transaction isolation levels, bulk operations, or query optimization).
- Name lookup methods consistently:
    - `get_xxx` returns the thing or a not-found error.
    - `find_xxx` returns `Option<T>` and treats absence as non-error.

## Domain Values And Views

- Prefer placing value-construction logic on the value type itself.
- If code builds a view/domain value from other domain values and does not depend on infrastructure, add an inherent constructor or `From` impl on that value type.
- When converting owned domain state into views, prefer move-based decomposition over cloning.
- If trait APIs only expose borrowed accessors, consider adding an owned decomposition method such as `into_parts(self)`.

## Event Modeling

- Prefer enums for event types when they improve type safety and clarity.
- Use descriptive event names.
- Implement serialization and deserialization traits for event enums used across boundaries.
- Keep event structures focused on the data they need to carry.
- Prefer separate structs for event data rather than large inline enum variants when it improves readability.
- Add comments only where usage or context is not obvious from names and types.

## Error Handling

- Prefer operation-specific error types and keep impossible variants out of operation-specific APIs when practical.
- Reuse shared domain errors for repeated concepts such as resource not found, type mismatch, and load failures.
- When translation is structural only, implement `From` near the error type definition and keep service/use-case code at a higher level.
- Do not use catch-all match arms for error conversion; list variants explicitly.
- Prefer plain `InternalError` for generic infrastructure/read failures.
- Introduce named wrappers only when the distinction is meaningful at the boundary.
- Prefer `.int_err()` plus `.with_context(...)` over ad hoc `InternalError::new(format!(...))`.

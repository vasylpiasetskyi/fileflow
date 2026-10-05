---
name: testing
description: Testing rules for FileFlow
alwaysApply: true
---

# Testing Rules

- pytest + pytest-asyncio (`asyncio_mode = auto`).
- Unit tests (`tests/`): state transitions, retry/backoff math, message
  validation, idempotency, file processing. No infrastructure needed.
- Integration tests (`tests/integration/`, `@pytest.mark.integration`): real
  PostgreSQL / MinIO / Kafka from docker compose. Prefer these over mocks for
  DB and messaging behavior.
- Mock or fake only at external boundaries (e.g. FastAPI dependency overrides).
- Failure experiments (kill/stop a service, verify recovery) are first-class:
  each phase ends with one, and its result is reported.
- Test names describe behavior: `test_duplicate_event_for_completed_task_is_noop`.
- Avoid fixed `sleep()` waits in tests; poll for a condition with a timeout.

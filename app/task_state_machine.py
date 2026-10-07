"""Task state machine: the single source of truth for allowed status changes.

    PENDING ──> PROCESSING ──> COMPLETED
       ▲                  ├──> FAILED
       │                  └──> RETRY ──┐   (retryable failure, attempts left)
       └───────────────────────────────┘   (recovery, once next_attempt_at is due)

The table is data, not scattered if-statements. Repositories turn it into a
guarded UPDATE (`WHERE status IN sources_of(new)`), so checking the rule and
applying the change happen atomically inside PostgreSQL.
"""

from app.models import TaskStatus

TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.PROCESSING}),
    TaskStatus.PROCESSING: frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.RETRY}),
    TaskStatus.RETRY: frozenset({TaskStatus.PENDING}),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


# Operator actions, never taken by the pipeline itself. FAILED stays terminal
# for workers (a late event for a FAILED task is a no-op); only an explicit
# DLQ redrive may bring it back.
MANUAL_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.FAILED: frozenset({TaskStatus.PENDING}),
}


def is_terminal(status: TaskStatus) -> bool:
    """No automatic transition leads out of this status: the pipeline is done with it."""
    return not TRANSITIONS[status]


def sources_of(new: TaskStatus, *, manual: bool = False) -> frozenset[TaskStatus]:
    """All statuses from which `new` may be entered (including operator actions if `manual`)."""
    tables = (TRANSITIONS, MANUAL_TRANSITIONS) if manual else (TRANSITIONS,)
    return frozenset(src for table in tables for src, targets in table.items() if new in targets)

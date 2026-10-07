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


def is_terminal(status: TaskStatus) -> bool:
    """No transition leads out of this status: the task is done for good."""
    return not TRANSITIONS[status]


def sources_of(new: TaskStatus) -> frozenset[TaskStatus]:
    """All statuses from which `new` may be entered."""
    return frozenset(src for src, targets in TRANSITIONS.items() if new in targets)

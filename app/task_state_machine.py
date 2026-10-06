"""Task state machine: the single source of truth for allowed status changes.

    PENDING ──> PROCESSING ──> COMPLETED
                          └──> FAILED

The table is data, not scattered if-statements. Repositories turn it into a
guarded UPDATE (`WHERE status IN sources_of(new)`), so checking the rule and
applying the change happen atomically inside PostgreSQL.
"""

from app.models import TaskStatus

TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.PROCESSING}),
    TaskStatus.PROCESSING: frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED}),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


def sources_of(new: TaskStatus) -> frozenset[TaskStatus]:
    """All statuses from which `new` may be entered."""
    return frozenset(src for src, targets in TRANSITIONS.items() if new in targets)

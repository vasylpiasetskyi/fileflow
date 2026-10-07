"""add task retry status and next_attempt_at

Revision ID: 0c9210317954
Revises: 7c960754cc9e
Create Date: 2026-10-07 15:13:53.478410

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0c9210317954"
down_revision: str | Sequence[str] | None = "7c960754cc9e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD = "'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'"
NEW = "'PENDING', 'PROCESSING', 'RETRY', 'COMPLETED', 'FAILED'"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("tasks", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index(
        "ix_tasks_retry_next_attempt_at",
        "tasks",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'RETRY'"),
    )
    # Written by hand: autogenerate does not compare CHECK constraints, so it
    # did not notice that the allowed statuses now include RETRY. Without this
    # the first PROCESSING -> RETRY update would fail with a CheckViolation.
    op.drop_constraint(op.f("ck_tasks_taskstatus"), "tasks", type_="check")
    op.create_check_constraint(op.f("ck_tasks_taskstatus"), "tasks", f"status IN ({NEW})")


def downgrade() -> None:
    """Downgrade schema.

    Fails (CheckViolation) while any task is in RETRY: move those first.
    """
    op.drop_constraint(op.f("ck_tasks_taskstatus"), "tasks", type_="check")
    op.create_check_constraint(op.f("ck_tasks_taskstatus"), "tasks", f"status IN ({OLD})")
    op.drop_index(
        "ix_tasks_retry_next_attempt_at",
        table_name="tasks",
        postgresql_where=sa.text("status = 'RETRY'"),
    )
    op.drop_column("tasks", "next_attempt_at")

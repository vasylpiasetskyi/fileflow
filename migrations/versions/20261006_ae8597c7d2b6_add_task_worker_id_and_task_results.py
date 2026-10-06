"""add task worker_id and task_results

Revision ID: ae8597c7d2b6
Revises: a003e52b9200
Create Date: 2026-10-06 09:19:38.308783

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ae8597c7d2b6"
down_revision: str | Sequence[str] | None = "a003e52b9200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "task_results",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("line_count", sa.BigInteger(), nullable=False),
        sa.Column("word_count", sa.BigInteger(), nullable=False),
        sa.Column("processing_ms", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], name=op.f("fk_task_results_task_id_tasks")
        ),
        sa.PrimaryKeyConstraint("task_id", name=op.f("pk_task_results")),
    )
    op.add_column("tasks", sa.Column("worker_id", sa.String(length=255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("tasks", "worker_id")
    op.drop_table("task_results")

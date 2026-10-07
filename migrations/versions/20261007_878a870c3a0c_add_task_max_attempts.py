"""add task max_attempts

Revision ID: 878a870c3a0c
Revises: 0c9210317954
Create Date: 2026-10-07 15:33:25.919106

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "878a870c3a0c"
down_revision: str | Sequence[str] | None = "0c9210317954"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # A NOT NULL column cannot be added to a table that has rows without saying
    # what those rows get. Existing tasks get the default budget (5) through a
    # temporary server default, which is then dropped: from now on the
    # application sets max_attempts explicitly for every task.
    op.add_column(
        "tasks", sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5")
    )
    op.alter_column("tasks", "max_attempts", server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("tasks", "max_attempts")

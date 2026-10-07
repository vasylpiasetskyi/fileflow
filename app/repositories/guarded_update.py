"""Guarded status transition shared by all repositories."""

import enum
import uuid
from collections.abc import Collection, Sequence
from typing import TypeVar

from sqlalchemy import ColumnElement, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Base

ModelT = TypeVar("ModelT", bound=Base)


async def transition_status(
    session: AsyncSession,
    model: type[ModelT],
    row_id: uuid.UUID,
    *,
    allowed_from: Collection[enum.Enum],
    new: enum.Enum,
    where: Sequence[ColumnElement[bool]] = (),
    **values: object,
) -> ModelT | None:
    """Atomically move a row to status `new` if it is currently in `allowed_from`.

        UPDATE <table> SET status = :new, ... WHERE id = :id AND status IN (:allowed_from)
        RETURNING *

    Under concurrency the first UPDATE locks the row; a competing UPDATE waits,
    then (READ COMMITTED) re-evaluates its WHERE against the committed row,
    matches nothing and returns None. Exactly one writer wins — no
    read-check-write gap like `SELECT status` + `if` + `UPDATE` would have.

    `where` adds conditions to the same atomic check (e.g. a fencing token).
    """
    stmt = (
        update(model)
        .where(model.id == row_id, model.status.in_(list(allowed_from)), *where)
        .values(status=new, **values)
        .returning(model)
    )
    return (await session.execute(stmt)).scalar_one_or_none()

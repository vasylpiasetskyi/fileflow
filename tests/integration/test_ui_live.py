"""The HTMX UI: page, fragments, upload, details, redrive."""

import uuid

import pytest
from fastapi import FastAPI
from sqlalchemy import update

from app.config import get_settings
from app.events import TaskQueuedEvent
from app.models import Task, TaskStatus
from app.services.processing_service import ProcessingService

pytestmark = pytest.mark.integration


async def _upload(client, name: str = "ui.txt", content: bytes = b"one two\nthree\n") -> dict:
    resp = await client.post("/files", files={"file": (name, content)})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _process(app: FastAPI, uploaded: dict) -> None:
    settings = get_settings().model_copy(update={"worker_id": "ui-worker"})
    await ProcessingService(app.state.sessionmaker, app.state.storage, settings).process(
        TaskQueuedEvent(task_id=uploaded["task"]["id"], file_id=uploaded["file"]["id"])
    )


async def test_page_has_the_upload_form_and_the_polling_table(client):
    resp = await client.get("/")

    assert resp.status_code == 200
    assert 'hx-post="/ui/files"' in resp.text
    assert 'hx-trigger="load, every 2s, tasks-changed from:body"' in resp.text
    assert '<option value="RETRY">' in resp.text


async def test_health_badges(client):
    resp = await client.get("/ui/health")

    assert resp.status_code == 200
    assert 'class="dep up"' in resp.text


async def test_task_table_lists_uploads_newest_first(client):
    assert "No tasks yet" in (await client.get("/ui/tasks")).text
    await _upload(client, "first.txt")
    await _upload(client, "second.txt")

    html = (await client.get("/ui/tasks")).text

    assert html.index("second.txt") < html.index("first.txt")
    assert 'class="badge pending"' in html
    assert "0/5" in html  # attempt / budget


async def test_task_table_status_filter(app, client):
    done = await _upload(client, "done.txt")
    await _upload(client, "waiting.txt")
    await _process(app, done)

    html = (await client.get("/ui/tasks", params={"status": "COMPLETED"})).text

    assert "done.txt" in html
    assert "waiting.txt" not in html


async def test_details_of_a_completed_task_show_the_result_and_stop_polling(app, client):
    uploaded = await _upload(client)
    await _process(app, uploaded)

    html = (await client.get(f"/ui/tasks/{uploaded['task']['id']}")).text

    assert 'class="badge completed"' in html
    assert "<dt>Words</dt><dd>3</dd>" in html
    assert "ui-worker" in html
    assert 'hx-trigger="every 2s"' not in html  # finished: nothing will change


async def test_details_of_a_pending_task_keep_polling(client):
    uploaded = await _upload(client)

    html = (await client.get(f"/ui/tasks/{uploaded['task']['id']}")).text

    assert 'hx-trigger="every 2s"' in html
    assert "Redrive" not in html


async def test_unknown_task_details(client):
    assert "Task not found" in (await client.get(f"/ui/tasks/{uuid.uuid4()}")).text


async def test_browser_upload_returns_a_fragment_and_refreshes_the_table(client):
    resp = await client.post("/ui/files", files={"file": ("browser.txt", b"hi\n")})

    assert resp.status_code == 200
    assert "browser.txt" in resp.text
    assert resp.headers["HX-Trigger"] == "tasks-changed"


async def test_upload_errors_are_shown_with_status_200_for_htmx(app, client):
    app.state.settings = app.state.settings.model_copy(update={"max_upload_bytes": 2})

    resp = await client.post("/ui/files", files={"file": ("big.txt", b"too big")})

    assert resp.status_code == 200  # HTMX would not swap a 413 into the page
    assert "Too large" in resp.text
    assert "HX-Trigger" not in resp.headers


async def test_failed_task_can_be_redriven_from_the_page(app, client):
    uploaded = await _upload(client, "bin.dat", b"bin\x00ary")
    await _process(app, uploaded)  # binary -> FAILED
    task_id = uploaded["task"]["id"]
    assert "Redrive" in (await client.get(f"/ui/tasks/{task_id}")).text

    resp = await client.post(f"/ui/tasks/{task_id}/redrive")

    assert "Redriven" in resp.text
    assert 'class="badge pending"' in resp.text
    assert resp.headers["HX-Trigger"] == "tasks-changed"


async def test_redriving_a_task_that_is_not_failed_explains_why(app, client):
    uploaded = await _upload(client)
    async with app.state.sessionmaker.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == uploaded["task"]["id"])
            .values(status=TaskStatus.COMPLETED)
        )

    resp = await client.post(f"/ui/tasks/{uploaded['task']['id']}/redrive")

    assert "Not redriven" in resp.text
    assert "COMPLETED" in resp.text

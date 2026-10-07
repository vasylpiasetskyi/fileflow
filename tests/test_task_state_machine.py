import pytest

from app.models import TaskStatus
from app.task_state_machine import TRANSITIONS, is_terminal, sources_of

P, R, C, F = TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.COMPLETED, TaskStatus.FAILED
RT = TaskStatus.RETRY


def test_every_status_has_an_entry_in_the_transition_table():
    assert set(TRANSITIONS) == set(TaskStatus)


@pytest.mark.parametrize(
    ("current", "new"),
    [(P, R), (R, C), (R, F), (R, RT), (RT, P)],
)
def test_allowed_transitions(current, new):
    assert new in TRANSITIONS[current]


@pytest.mark.parametrize(
    ("current", "new"),
    [(P, C), (P, F), (P, RT), (R, P), (RT, R), (RT, C), (C, R), (C, F), (F, R), (F, C)],
)
def test_forbidden_transitions(current, new):
    assert new not in TRANSITIONS[current]


@pytest.mark.parametrize("terminal", [C, F])
def test_terminal_statuses_have_no_way_out(terminal):
    assert TRANSITIONS[terminal] == frozenset()


def test_sources_of_inverts_the_table():
    assert sources_of(R) == {P}
    assert sources_of(C) == {R}
    assert sources_of(F) == {R}
    assert sources_of(P) == {RT}  # only a due retry re-enters PENDING
    assert sources_of(RT) == {R}


def test_is_terminal_marks_finished_statuses():
    assert {s for s in TaskStatus if is_terminal(s)} == {C, F}

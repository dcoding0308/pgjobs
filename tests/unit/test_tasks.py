import threading

import pytest

from pgjobs import ConfigurationError, DuplicateTaskError, Job, JobApp
from pgjobs.tasks import Task, TaskRegistry


def make_app() -> JobApp:
    # The pool is created lazily, so no database is needed for these tests.
    return JobApp("postgresql://unused/unused")


module_app = make_app()


@module_app.task()
async def module_level_task(job: Job) -> None: ...


def test_decorator_registers_task_with_default_name() -> None:
    assert module_level_task.name == f"{__name__}.module_level_task"
    assert module_app.registry.get(module_level_task.name) is module_level_task


def test_nested_functions_need_an_explicit_name() -> None:
    # "<locals>" names are not stable across processes, so they are rejected.
    app = make_app()
    with pytest.raises(ConfigurationError):

        @app.task()
        async def nested(job: Job) -> None: ...


def test_decorator_applies_options() -> None:
    app = make_app()

    @app.task("emails.send", queue="emails", max_attempts=2, timeout_seconds=5)
    async def send_email(job: Job) -> None: ...

    assert (send_email.queue, send_email.max_attempts, send_email.timeout_seconds) == (
        "emails",
        2,
        5,
    )


def test_duplicate_names_are_rejected() -> None:
    registry = TaskRegistry()
    registry.register(Task(name="a", handler=lambda job: None))
    with pytest.raises(DuplicateTaskError):
        registry.register(Task(name="a", handler=lambda job: None))


@pytest.mark.parametrize("name", ["", "has space", "1starts_with_digit", "x" * 201])
def test_invalid_names_are_rejected(name: str) -> None:
    with pytest.raises(ConfigurationError):
        Task(name=name, handler=lambda job: None)


@pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"timeout_seconds": 0}])
def test_invalid_settings_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ConfigurationError):
        Task(name="ok", handler=lambda job: None, **kwargs)  # type: ignore[arg-type]


async def test_sync_handlers_run_off_the_event_loop() -> None:
    seen_threads: list[int] = []
    task = Task(name="sync", handler=lambda job: seen_threads.append(threading.get_ident()))
    await task.run(None)  # type: ignore[arg-type]
    assert seen_threads and seen_threads[0] != threading.get_ident()


async def test_enqueue_requires_an_attached_app() -> None:
    task = Task(name="orphan", handler=lambda job: None)
    with pytest.raises(ConfigurationError):
        await task.enqueue({})


async def test_enqueue_validates_payload_before_touching_the_database() -> None:
    app = make_app()
    with pytest.raises(TypeError):
        await app.enqueue("anything", {"when": object()})
    with pytest.raises(TypeError):
        await app.enqueue("anything", {"bad": float("nan")})


async def test_enqueue_validates_options() -> None:
    app = make_app()
    with pytest.raises(ValueError):
        await app.enqueue("anything", priority=40_000)
    with pytest.raises(ValueError):
        await app.enqueue("anything", delay_seconds=-1)


async def test_explicit_zero_max_attempts_is_rejected() -> None:
    # Regression: `max_attempts or default` silently replaced 0 with the default.
    with pytest.raises(ValueError):
        await make_app().enqueue("anything", max_attempts=0)


async def test_empty_queue_name_is_rejected() -> None:
    with pytest.raises(ValueError):
        await make_app().enqueue("anything", queue="")


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_non_finite_task_timeout_is_rejected(value: float) -> None:
    with pytest.raises(ConfigurationError):
        Task(name="ok", handler=lambda job: None, timeout_seconds=value)

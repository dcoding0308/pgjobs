"""Command-line interface.

    pgjobs migrate
    pgjobs worker myproject.jobs:app --queue default --concurrency 10
    pgjobs enqueue myproject.jobs.send_email --payload '{"to": "a@example.com"}'
    pgjobs stats
    pgjobs requeue-dead [--queue NAME]

The database URL comes from ``--database-url``, then ``PGJOBS_DATABASE_URL``,
then ``DATABASE_URL``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import sys
from collections.abc import Sequence

from pgjobs.app import JobApp
from pgjobs.errors import ConfigurationError
from pgjobs.worker import Worker


def load_app(import_path: str) -> JobApp:
    """Load a JobApp from a ``module.path:attribute`` string."""
    module_name, sep, attribute = import_path.partition(":")
    if not sep or not module_name or not attribute:
        raise ConfigurationError(f"expected 'module:attribute', got {import_path!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigurationError(f"cannot import module {module_name!r}: {exc}") from exc
    app = getattr(module, attribute, None)
    if not isinstance(app, JobApp):
        raise ConfigurationError(f"{import_path!r} is not a JobApp instance")
    return app


def resolve_database_url(explicit: str | None) -> str:
    url = explicit or os.environ.get("PGJOBS_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise ConfigurationError(
            "no database URL: pass --database-url or set PGJOBS_DATABASE_URL / DATABASE_URL"
        )
    return url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pgjobs", description="PostgreSQL-backed job queue")
    parser.add_argument("--database-url", help="PostgreSQL connection URL")
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("migrate", help="create or update the database schema")

    worker = commands.add_parser("worker", help="run a worker process")
    worker.add_argument("app", help="import path of the JobApp, e.g. myproject.jobs:app")
    worker.add_argument(
        "--queue", action="append", dest="queues", help="queue to consume (repeatable)"
    )
    worker.add_argument("--concurrency", type=int, default=10)
    worker.add_argument("--lease-seconds", type=float, default=30.0)
    worker.add_argument("--poll-interval", type=float, default=5.0)
    worker.add_argument("--reap-interval", type=float, default=10.0)
    worker.add_argument("--shutdown-grace", type=float, default=30.0)
    worker.add_argument(
        "--no-listen",
        action="store_true",
        help="poll only; required behind poolers that do not support LISTEN",
    )

    enqueue = commands.add_parser("enqueue", help="enqueue a single job")
    enqueue.add_argument("task_name")
    enqueue.add_argument("--payload", default="{}", help="JSON object")
    enqueue.add_argument("--queue", default=None)
    enqueue.add_argument("--priority", type=int, default=0)
    enqueue.add_argument("--delay", type=float, default=0.0, help="seconds to wait before running")
    enqueue.add_argument("--max-attempts", type=int, default=None)
    enqueue.add_argument("--dedupe-key", default=None)

    commands.add_parser("stats", help="show job counts per queue and status")

    requeue = commands.add_parser("requeue-dead", help="retry dead jobs with fresh attempts")
    requeue.add_argument("--queue", default=None)

    dashboard = commands.add_parser("dashboard", help="serve the web dashboard")
    dashboard.add_argument(
        "--host", default="127.0.0.1", help="interface to bind (default: localhost)"
    )
    dashboard.add_argument("--port", type=int, default=8080)
    dashboard.add_argument(
        "--read-only", action="store_true", help="disable the 'retry dead jobs' action"
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    if args.command == "worker":
        app = load_app(args.app)
        if args.database_url:
            raise ConfigurationError("the worker uses the database URL configured on its JobApp")
    else:
        pool_size = 5 if args.command == "dashboard" else 2
        app = JobApp(resolve_database_url(args.database_url), pool_max_size=pool_size)

    async with app:
        if args.command == "migrate":
            await app.migrate()
            print("schema is up to date")
        elif args.command == "worker":
            worker = Worker(
                app,
                queues=args.queues or ["default"],
                concurrency=args.concurrency,
                lease_seconds=args.lease_seconds,
                poll_interval_seconds=args.poll_interval,
                reap_interval_seconds=args.reap_interval,
                shutdown_grace_seconds=args.shutdown_grace,
                use_listen_notify=not args.no_listen,
            )
            worker.install_signal_handlers()
            await worker.run()
        elif args.command == "enqueue":
            payload = json.loads(args.payload)
            result = await app.enqueue(
                args.task_name,
                payload,
                queue=args.queue,
                priority=args.priority,
                delay_seconds=args.delay,
                max_attempts=args.max_attempts,
                dedupe_key=args.dedupe_key,
            )
            state = "created" if result.created else "already exists"
            print(f"job {result.job_id} ({state})")
        elif args.command == "stats":
            stats = await app.stats()
            if not stats:
                print("no jobs")
            for queue, counts in stats.items():
                summary = "  ".join(f"{status}={count}" for status, count in sorted(counts.items()))
                print(f"{queue}: {summary}")
        elif args.command == "requeue-dead":
            count = await app.requeue_dead(args.queue)
            print(f"requeued {count} job(s)")
        elif args.command == "dashboard":
            await _serve_dashboard(app, args)
    return 0


async def _serve_dashboard(app: JobApp, args: argparse.Namespace) -> None:
    try:
        import uvicorn

        from pgjobs.dashboard import create_dashboard
    except ImportError as exc:
        raise ConfigurationError(
            'the dashboard needs extra packages: pip install "pgjobs[dashboard]"'
        ) from exc
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        logging.getLogger("pgjobs.dashboard").warning(
            "dashboard has no authentication and is listening on %s; put it behind your own auth",
            args.host,
        )
    dashboard = create_dashboard(app, allow_actions=not args.read_only)
    config = uvicorn.Config(dashboard, host=args.host, port=args.port, log_level="warning")
    print(f"dashboard on http://{args.host}:{args.port}  (Ctrl+C to stop)")
    await uvicorn.Server(config).serve()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # Allow `pgjobs worker myapp:app` to import modules from the current directory.
    if "" not in sys.path and os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    try:
        return asyncio.run(_run(args))
    except ConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        print(f"error: --payload is not valid JSON: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130

"""A realistic mixed workload for demos and the dashboard.

Four kinds of jobs on four queues:

* media     images.thumbnail       fast, always succeeds
* payments  payments.sync_webhook  flaky partner API (30% failures), retried
* reports   reports.monthly        slow (2-4 s), uses heartbeats
* imports   imports.csv_row        some rows are invalid and end up dead

    export DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs
    uv run pgjobs migrate
    uv run pgjobs worker examples.demo:app --queue media --queue payments \\
        --queue reports --queue imports --concurrency 20
    uv run python examples/demo.py produce --seconds 60 --rate 10
    uv run pgjobs dashboard
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random

from pgjobs import BackoffPolicy, Job, JobApp

app = JobApp(os.environ.get("DATABASE_URL", "postgresql://pgjobs:pgjobs@localhost:5433/pgjobs"))


@app.task("images.thumbnail", queue="media")
async def make_thumbnail(job: Job) -> None:
    await asyncio.sleep(random.uniform(0.1, 0.6))


@app.task(
    "payments.sync_webhook",
    queue="payments",
    max_attempts=4,
    backoff=BackoffPolicy(base_seconds=1, max_seconds=5),
)
async def sync_payment_webhook(job: Job) -> None:
    await asyncio.sleep(random.uniform(0.05, 0.3))
    if random.random() < 0.3:
        raise ConnectionError("payment partner returned 503 Service Unavailable")


@app.task("reports.monthly", queue="reports", timeout_seconds=10)
async def build_monthly_report(job: Job) -> None:
    await asyncio.sleep(random.uniform(2, 4))


@app.task(
    "imports.csv_row",
    queue="imports",
    max_attempts=3,
    backoff=BackoffPolicy(base_seconds=0.5, max_seconds=2),
)
async def import_csv_row(job: Job) -> None:
    await asyncio.sleep(random.uniform(0.05, 0.2))
    if job.payload.get("email", "").count("@") != 1:
        raise ValueError(f"row {job.payload['row']}: invalid email {job.payload.get('email')!r}")


async def produce(seconds: float, rate: float) -> None:
    async with app:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        counter = 0
        while loop.time() < deadline:
            counter += 1
            kind = random.choices(["media", "payments", "reports", "imports"], [5, 3, 1, 2])[0]
            if kind == "media":
                await make_thumbnail.enqueue({"image_id": counter, "size": "256x256"})
            elif kind == "payments":
                await sync_payment_webhook.enqueue(
                    {"payment_id": f"pay_{counter:05d}"}, dedupe_key=f"pay_{counter:05d}"
                )
            elif kind == "reports":
                await build_monthly_report.enqueue(
                    {"account_id": random.randint(1, 50)}, priority=-1
                )
            else:
                bad = random.random() < 0.15
                email = f"user{counter}.example.com" if bad else f"user{counter}@example.com"
                await import_csv_row.enqueue({"row": counter, "email": email})
            await asyncio.sleep(random.expovariate(rate))
        print(f"enqueued {counter} jobs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    producer = commands.add_parser("produce", help="enqueue a stream of mixed jobs")
    producer.add_argument("--seconds", type=float, default=60)
    producer.add_argument("--rate", type=float, default=10, help="jobs per second on average")
    args = parser.parse_args()
    asyncio.run(produce(args.seconds, args.rate))

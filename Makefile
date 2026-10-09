DATABASE_URL ?= postgresql://pgjobs:pgjobs@localhost:5433/pgjobs
export DATABASE_URL
# Tests and benchmarks truncate the jobs table, so they get their own
# database and never interfere with a running demo.
export PGJOBS_TEST_DATABASE_URL ?= postgresql://pgjobs:pgjobs@localhost:5433/pgjobs_test
export PGJOBS_BENCH_DATABASE_URL ?= $(PGJOBS_TEST_DATABASE_URL)

DEMO_QUEUES = --queue media --queue payments --queue reports --queue imports
LOG_DIR = .demo-logs

.PHONY: help db db-down install check test chaos bench demo demo-stop

help:
	@echo "make db         start PostgreSQL 16 in Docker (localhost:5433)"
	@echo "make install    install dependencies with uv"
	@echo "make check      lint, format check, type check"
	@echo "make test       unit + integration tests"
	@echo "make chaos      crash test (kills worker processes)"
	@echo "make bench      throughput and latency benchmarks (truncates jobs)"
	@echo "make demo       run worker + dashboard + mixed workload"
	@echo "make demo-stop  stop the demo processes"
	@echo "make db-down    stop and remove the database container"

db:
	docker compose up -d --wait
	@docker compose exec -T postgres psql -U pgjobs -d pgjobs -tAc \
		"SELECT 1 FROM pg_database WHERE datname = 'pgjobs_test'" | grep -q 1 || \
		docker compose exec -T postgres createdb -U pgjobs pgjobs_test

db-down:
	docker compose down -v

install:
	uv sync

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test:
	uv run pytest -m "not chaos"

chaos:
	uv run pytest -m chaos -s

bench:
	uv run python benchmarks/run.py

demo: db
	uv run pgjobs migrate
	@mkdir -p $(LOG_DIR)
	@nohup uv run pgjobs worker examples.demo:app $(DEMO_QUEUES) --concurrency 20 \
		> $(LOG_DIR)/worker.log 2>&1 & echo $$! > $(LOG_DIR)/worker.pid
	@nohup uv run pgjobs dashboard > $(LOG_DIR)/dashboard.log 2>&1 & echo $$! > $(LOG_DIR)/dashboard.pid
	@nohup uv run python examples/demo.py produce --seconds 600 --rate 8 \
		> $(LOG_DIR)/producer.log 2>&1 & echo $$! > $(LOG_DIR)/producer.pid
	@echo "dashboard: http://127.0.0.1:8080   logs: $(LOG_DIR)/   stop: make demo-stop"

demo-stop:
	@for name in producer worker dashboard; do \
		if [ -f $(LOG_DIR)/$$name.pid ]; then kill $$(cat $(LOG_DIR)/$$name.pid) 2>/dev/null || true; \
		rm -f $(LOG_DIR)/$$name.pid; fi; \
	done
	@echo "demo stopped"

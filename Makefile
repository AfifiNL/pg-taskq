# Everything here runs offline and costs nothing. `make check` is exactly what CI runs.
# Tests use PGTASKQ_TEST_DATABASE_URL when it is set (CI: a PostgreSQL service container).
# Otherwise they start a throwaway PostgreSQL from the pip-installed `pgserver` package
# (dependency group `localpg`), so no Docker and no system install is needed.
UV ?= uv

ifdef PGTASKQ_TEST_DATABASE_URL
PG_GROUP :=
else
PG_GROUP := --group localpg
endif

.PHONY: help install lint fmt typecheck test check demo

help:            ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

install:         ## create .venv with dev tools and the bundled PostgreSQL (locked)
	$(UV) sync --group localpg

lint:            ## ruff lint + format check
	$(UV) run ruff check src tests examples
	$(UV) run ruff format --check src tests examples

fmt:             ## apply ruff formatting and safe fixes
	$(UV) run ruff format src tests examples
	$(UV) run ruff check --fix src tests examples

typecheck:       ## mypy (strict)
	$(UV) run mypy

test:            ## pytest, including the PostgreSQL integration and property tests
	$(UV) run $(PG_GROUP) pytest

check: lint typecheck test demo  ## everything CI runs

demo:            ## 3 workers, retries, a crashed worker, a poison job and the outbox, end to end
	$(UV) run $(PG_GROUP) python examples/demo.py

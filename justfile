set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

setup:
    script/setup.sh

lint *args:
    uv run ruff check {{args}}

typecheck *args:
    uv run pyright {{args}}

test *args:
    uv run pytest {{args}}

check: lint typecheck test

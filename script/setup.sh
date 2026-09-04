#!/usr/bin/env bash
# script/setup
# Prepare a fresh clone for development: environment, hooks.
set -euo pipefail

uv sync
git config core.hooksPath script/hook
chmod +x script/hook/*
echo "setup complete"
#!/bin/bash
# run_lint.sh - Static Code Analysis Runner
# This script runs flake8 to check for style and syntax issues.
set -e

echo "=== Running Static Code Analysis (Linting) with flake8 ==="

# --count: show count of warnings/errors
# --select=E9,F63,F7,F82: select errors and warnings to report (E9=syntax, F=pyflakes)
# --show-source: show the source code for each error
# --statistics: show counts of each error/warning type
# --max-line-length=88: a common modern line length
flake8 services/ tests/ \
    --count \
    --select=E9,F63,F7,F82 \
    --show-source \
    --statistics \
    --max-line-length=88

echo "=== Linting Passed Successfully ==="

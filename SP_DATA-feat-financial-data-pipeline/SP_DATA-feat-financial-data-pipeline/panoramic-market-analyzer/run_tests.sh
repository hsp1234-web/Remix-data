#!/bin/bash
# run_tests.sh - Test Runner
# This script runs all pytest tests.
set -e

echo "=== Running all tests ==="

# The -v flag is for verbose output
pytest -v tests/

echo "=== All tests passed successfully ==="

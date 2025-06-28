#!/bin/bash
# run_quality_checks.sh - Master Quality Gate
# This script runs all quality assurance steps in order.
# It will exit immediately if any step fails.
set -e

echo "================================="
echo "  STARTING QUALITY ASSURANCE   "
echo "================================="

# --- Step 1: Run Linting ---
echo
echo "--- Step 1: Running Static Code Analysis ---"
bash run_lint.sh

# --- Step 2: Run Unit Tests ---
echo
echo "--- Step 2: Running Unit Tests ---"
bash run_tests.sh

echo
echo "================================="
echo "  ALL QUALITY CHECKS PASSED!   "
echo "================================="

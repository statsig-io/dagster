#!/bin/bash

# Quick test script for pydantic v2 migration
set -e

echo "=== Quick Pydantic v2 Migration Test ==="

# Create and activate virtual environment
echo "Creating test environment..."
python3 -m venv venv_pydantic_test
source venv_pydantic_test/bin/activate

# Install pydantic v2
echo "Installing pydantic v2..."
pip install "pydantic>=2.0.0,<3.0.0" --quiet

# Install dagster in development mode
echo "Installing dagster from current branch..."
pip install -e python_modules/dagster/ --quiet

# Run the comprehensive test
echo "Running pydantic v2 compatibility tests..."
python test_pydantic_v2.py

# Cleanup
echo "Cleaning up..."
deactivate
rm -rf venv_pydantic_test

echo "=== Test Complete ==="
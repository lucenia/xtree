#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Script to run performance benchmarks and regression tests

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../../../.."
BUILD_DIR="$PROJECT_ROOT/build/native"

echo "XTree Performance Benchmarks"
echo "============================"
echo

# Check if we need to build
if [ ! -f "$BUILD_DIR/bin/xtree_tests" ]; then
    echo "Building XTree first..."
    cd "$PROJECT_ROOT"
    ./gradlew build
    echo
fi

cd "$PROJECT_ROOT"

# Run all performance tests
echo "Running all performance tests..."
./build/native/bin/xtree_tests --gtest_filter="*Performance*" --gtest_color=yes

# Run specific benchmark if requested
if [ "$1" == "regression" ]; then
    echo -e "\nRunning performance regression check..."
    # Temporarily enable the regression test
    ./build/native/bin/xtree_tests --gtest_filter="PerformanceRegressionTest.CheckPerformanceRegression" --gtest_also_run_disabled_tests --gtest_color=yes
elif [ "$1" == "update-baseline" ]; then
    echo -e "\nUpdating performance baseline..."
    UPDATE_PERFORMANCE_BASELINE=1 ./build/native/bin/xtree_tests --gtest_filter="PerformanceRegressionTest.CheckPerformanceRegression" --gtest_also_run_disabled_tests --gtest_color=yes
fi

echo -e "\nUsage:"
echo "  ./benchmarks/run_benchmarks.sh           # Run all performance tests"
echo "  ./benchmarks/run_benchmarks.sh regression # Check for performance regressions"
echo "  ./benchmarks/run_benchmarks.sh update-baseline # Update the baseline"
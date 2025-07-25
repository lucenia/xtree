# Benchmarks for XTree
# These are separate from regular tests to avoid running them in CI by default

# Add benchmark sources to the test executable
# They are disabled by default using DISABLED_ prefix in test names
set(BENCHMARK_SOURCES
    benchmarks/performance_regression.cpp
)

# The benchmarks are compiled as part of xtree_tests but disabled by default
# To run them explicitly:
#   ./build/native/bin/xtree_tests --gtest_filter="*Performance*" --gtest_also_run_disabled_tests
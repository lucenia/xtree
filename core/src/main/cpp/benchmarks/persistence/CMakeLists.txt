# Persistence Layer Benchmarks

set(PERSISTENCE_BENCH_SOURCES
    bench_wal_performance.cpp
    bench_checkpoint_performance.cpp
    bench_recovery_performance.cpp
    bench_segment_allocator.cpp
    bench_group_commit.cpp
    bench_durability_modes.cpp
)

# Create individual benchmark executables
foreach(bench_source ${PERSISTENCE_BENCH_SOURCES})
    get_filename_component(bench_name ${bench_source} NAME_WE)
    add_executable(${bench_name} ${bench_source})
    
    target_link_libraries(${bench_name}
        xtree_persistence
        gtest
        gtest_main
        ${CMAKE_THREAD_LIBS_INIT}
    )
    
    target_include_directories(${bench_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/persistence
    )
    
    # Add as test
    add_test(NAME ${bench_name} COMMAND ${bench_name})
endforeach()

# Combined benchmark suite
add_executable(persistence_bench_suite
    ${PERSISTENCE_BENCH_SOURCES}
    bench_main.cpp
)

target_link_libraries(persistence_bench_suite
    xtree_persistence
    gtest
    ${CMAKE_THREAD_LIBS_INIT}
)

target_include_directories(persistence_bench_suite PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/persistence
)

# Benchmark runner script
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/run_benchmarks.sh
    ${CMAKE_CURRENT_BINARY_DIR}/run_benchmarks.sh
    COPYONLY
)

add_library(raft_core STATIC
    raft_node.cpp
    raft_rpc_client.cpp
    raft_rpc.cpp
    persister.cpp
    mem_transport.cpp
    fault_injector.cpp
    invariant_checker.cpp
    test_harness.cpp
)

target_include_directories(raft_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(raft_core
    PUBLIC
        raft_proto
        gRPC::grpc++
        kv_store
        thread_pool
    PRIVATE
        Threads::Threads
)

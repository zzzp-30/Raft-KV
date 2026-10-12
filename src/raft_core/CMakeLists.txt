
set(RAFT_CORE_SOURCES
    raft_node.cpp
    persister.cpp
    mem_transport.cpp
    fault_injector.cpp
    invariant_checker.cpp
    test_harness.cpp
)

if(BUILD_RAFT_GRPC)
    list(APPEND RAFT_CORE_SOURCES
        raft_rpc_client.cpp
        raft_rpc.cpp
    )
endif()

add_library(raft_core STATIC ${RAFT_CORE_SOURCES})

target_include_directories(raft_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(raft_core
    PUBLIC
        raft_proto_msg
        kv_store
        thread_pool
    PRIVATE
        Threads::Threads
)

if(BUILD_RAFT_GRPC)
    target_link_libraries(raft_core PUBLIC raft_proto gRPC::grpc++)
endif()

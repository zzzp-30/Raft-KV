add_executable(raft_example
    main.cpp
)


target_link_libraries(raft_example
    PRIVATE
        raft_core
        raft_storage
        kv_store
)

if(BUILD_RAFT_GRPC)
    add_executable(server_test
        server_test.cpp
    )
    target_link_libraries(server_test
        PRIVATE
            Server
    )
    add_executable(client_test
        client_test.cpp
    )
    target_link_libraries(client_test
        PRIVATE
            Client
    )
endif()

add_executable(thread_pool_test
    thread_pool_test.cpp
)
target_link_libraries(thread_pool_test
    PRIVATE
        thread_pool
)

add_executable(raft_test
    raft_test.cpp
)
target_link_libraries(raft_test
    PRIVATE
        raft_core
        kv_store
)

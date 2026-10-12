find_package(Protobuf REQUIRED)
SET(PROTO_FILE raft_rpc.proto)

# Message-only library: protobuf serialization, no gRPC dependency.
# raft_core links this — proves core compiles without gRPC.
protobuf_generate(
    LANGUAGE cpp
    OUT_VAR PB_SRCS
    PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOS ${PROTO_FILE}
)

add_library(raft_proto_msg STATIC ${PB_SRCS})
target_include_directories(raft_proto_msg PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(raft_proto_msg PUBLIC protobuf::libprotobuf)

# gRPC service library: only when BUILD_RAFT_GRPC=ON.
if(BUILD_RAFT_GRPC)
    find_package(gRPC CONFIG REQUIRED)
    protobuf_generate(
        LANGUAGE grpc
        OUT_VAR GRPC_SRCS
        PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
        PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
        GENERATE_EXTENSIONS .grpc.pb.cc .grpc.pb.h
        PROTOS ${PROTO_FILE}
    )

    add_library(raft_proto STATIC ${GRPC_SRCS})
    target_include_directories(raft_proto PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(raft_proto PUBLIC raft_proto_msg gRPC::grpc++)
endif()

#pragma once
#ifndef RAFTRPC_H
#define RAFTRPC_H
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <string>

#include "raft_rpc.grpc.pb.h"
#include "raft_rpc.pb.h"
#include "raft_messages.h"

class RaftNode;

class RaftRPCServiceImpl final : public raft::RaftRPC::Service {
private:
    RaftNode& raft_node_;
public:
    RaftRPCServiceImpl(RaftNode&raft_node):raft_node_(raft_node){}
    grpc::Status OnHeartBeat(grpc::ServerContext*,
                          const raft::HeartBeatRequest* request,
                          raft::HeartBeatReply* response) override;
    grpc::Status OnRequestVote(grpc::ServerContext*,
                          const raft::VoteRequest* request,
                          raft::VoteReply* response) override;
    grpc::Status OnInstallSnapshot(grpc::ServerContext*,
                          const raft::InstallSnapshotRequest* request,
                          raft::InstallSnapshotReply* response) override;
};

class RaftServer
{
private:
    std::unique_ptr<grpc::Server> server_;
public:
    void Start(const std::string&address,RaftNode&raft_node);
};

#endif //!RAFTRPC_H

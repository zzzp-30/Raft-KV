#pragma once
#ifndef RAFTRPCCLIENT_H
#define RAFTRPCCLIENT_H

#include "raft_rpc.grpc.pb.h"
#include "raft_rpc.pb.h"
#include "i_transport.h"
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

class RaftRPCClient : public ITransport
{
private:
    std::unique_ptr<raft::RaftRPC::Stub>stub_;
public:
    explicit RaftRPCClient(const std::string& address)
        : stub_(raft::RaftRPC::NewStub(
              grpc::CreateChannel(
                  address,
                  grpc::InsecureChannelCredentials()))) {}
    bool SendHeartBeat(const raft_msg::HeartBeatRequest &request,raft_msg::HeartBeatReply*response) override;     
    bool SendRequestVote(const raft_msg::VoteRequest &request,raft_msg::VoteReply*response) override;       
    bool SendInstallSnapshot(const raft_msg::InstallSnapshotRequest &request,raft_msg::InstallSnapshotReply*response) override;       
};

#endif //!RAFTRPCCLIENT_H

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

//定义RPC客户端
class RaftRPCClient : public ITransport
{
private:
    std::unique_ptr<raft::RaftRPC::Stub>stub_;  //存根
public:
    explicit RaftRPCClient(const std::string& address)
        : stub_(raft::RaftRPC::NewStub(
              grpc::CreateChannel(
                  address,
                  grpc::InsecureChannelCredentials()))) {}
    bool SendHeartBeat(const raft::HeartBeatRequest &request,raft::HeartBeatReply*response) override;     
    bool SendRequestVote(const raft::VoteRequest &request,raft::VoteReply*response) override;       
    bool SendInstallSnapshot(const raft::InstallSnapshotRequest &request,raft::InstallSnapshotReply*response) override;       

};


#endif //!RAFTRPCCLIENT_H

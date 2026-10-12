#include "raft_rpc_client.h"
bool RaftRPCClient::SendHeartBeat(const raft::HeartBeatRequest &request,raft::HeartBeatReply*response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(3));
    //调用RPC
    const grpc::Status kStatus = stub_->OnHeartBeat(&context,request,response);
    if (!kStatus.ok()) {
        // std::cerr << "RPC failed: "
        //       << status.error_message() << '\n';
        return false;
    }
    return true;
}
bool RaftRPCClient::SendRequestVote(const raft::VoteRequest &request,raft::VoteReply*response){  
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(3));
    //调用RPC
    const grpc::Status kStatus = stub_->OnRequestVote(&context,request,response);
    if (!kStatus.ok()) {
        // std::cerr << "RPC failed: "
        //       << status.error_message() << '\n';
        return false;
    }
    return true;
}
bool RaftRPCClient::SendInstallSnapshot(const raft::InstallSnapshotRequest &request,raft::InstallSnapshotReply*response){
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(10));
    const grpc::Status kStatus = stub_->OnInstallSnapshot(&context,request,response);
    if (!kStatus.ok()) {
        return false;
    }
    return true;
}

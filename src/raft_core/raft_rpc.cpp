
#include "raft_rpc.h"
#include "raft_rpc.pb.h"
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <thread>
#include"raft_node.h"
grpc::Status RaftRPCServiceImpl::OnHeartBeat(grpc::ServerContext*,
                          const raft::HeartBeatRequest* request,
                          raft::HeartBeatReply* response) {
    raft_node_.OnHeartBeat(request,response);
    return grpc::Status::OK;
}
void RaftServer::Start(const std::string&address,RaftNode&raft_node)
{
    std::thread th(
        [this,raft_node = &raft_node,address](){
            RaftRPCServiceImpl service(*raft_node);
            grpc::ServerBuilder builder;
            builder.AddListeningPort(
                address,
                grpc::InsecureServerCredentials());
            builder.RegisterService(&service);

            server_ = builder.BuildAndStart();
            if (!server_) {
                std::cerr << "failed to start gRPC server\n";
                return;
            }

            std::cout << "gRPC server listening on " << address << '\n';
            server_->Wait();
        }
    );
    th.detach();
   
}

grpc::Status RaftRPCServiceImpl::OnRequestVote(grpc::ServerContext*,
                          const raft::VoteRequest* request,
                          raft::VoteReply* response) {
    
    raft_node_.OnRequestVote(request,response);
    return grpc::Status::OK;
}

grpc::Status RaftRPCServiceImpl::OnInstallSnapshot(grpc::ServerContext*,
                          const raft::InstallSnapshotRequest* request,
                          raft::InstallSnapshotReply* response) {
    raft_node_.OnInstallSnapshot(request,response);
    return grpc::Status::OK;
}

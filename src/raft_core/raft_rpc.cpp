#include "raft_rpc.h"
#include "raft_node.h"
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <thread>

namespace {
void FromProto(const raft::HeartBeatRequest& src, raft_msg::HeartBeatRequest& dst) {
    dst.term = src.term();
    dst.leader_id = src.leader_id();
    dst.prev_index = src.prev_index();
    dst.prev_term = src.prev_term();
    dst.commited_index = src.commited_index();
    dst.entries.clear();
    for (int i = 0; i < src.entries_size(); ++i) {
        const auto& e = src.entries(i);
        dst.entries.push_back({e.index(), e.term(), e.cmd()});
    }
}
void ToProto(const raft_msg::HeartBeatReply& src, raft::HeartBeatReply& dst) {
    dst.set_term(src.term);
    dst.set_success(src.success);
    dst.set_next_index(src.next_index);
}
void FromProto(const raft::VoteRequest& src, raft_msg::VoteRequest& dst) {
    dst.term = src.term();
    dst.candidate_id = src.candidate_id();
    dst.last_log_index = src.last_log_index();
    dst.last_log_term = src.last_log_term();
}
void ToProto(const raft_msg::VoteReply& src, raft::VoteReply& dst) {
    dst.set_term(src.term);
    dst.set_vote_granted(src.vote_granted);
}
void FromProto(const raft::InstallSnapshotRequest& src, raft_msg::InstallSnapshotRequest& dst) {
    dst.term = src.term();
    dst.leader_id = src.leader_id();
    dst.last_snapshot_index = src.last_snapshot_index();
    dst.last_snapshot_term = src.last_snapshot_term();
    dst.snapshot_data = src.snapshot_data();
}
void ToProto(const raft_msg::InstallSnapshotReply& src, raft::InstallSnapshotReply& dst) {
    dst.set_term(src.term);
    dst.set_success(src.success);
}
}  // namespace

grpc::Status RaftRPCServiceImpl::OnHeartBeat(grpc::ServerContext*,
                          const raft::HeartBeatRequest* request,
                          raft::HeartBeatReply* response) {
    raft_msg::HeartBeatRequest internal_req;
    raft_msg::HeartBeatReply internal_resp;
    FromProto(*request, internal_req);
    raft_node_.OnHeartBeat(&internal_req, &internal_resp);
    ToProto(internal_resp, *response);
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
    raft_msg::VoteRequest internal_req;
    raft_msg::VoteReply internal_resp;
    FromProto(*request, internal_req);
    raft_node_.OnRequestVote(&internal_req, &internal_resp);
    ToProto(internal_resp, *response);
    return grpc::Status::OK;
}

grpc::Status RaftRPCServiceImpl::OnInstallSnapshot(grpc::ServerContext*,
                          const raft::InstallSnapshotRequest* request,
                          raft::InstallSnapshotReply* response) {
    raft_msg::InstallSnapshotRequest internal_req;
    raft_msg::InstallSnapshotReply internal_resp;
    FromProto(*request, internal_req);
    raft_node_.OnInstallSnapshot(&internal_req, &internal_resp);
    ToProto(internal_resp, *response);
    return grpc::Status::OK;
}

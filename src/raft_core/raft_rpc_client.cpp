#include "raft_rpc_client.h"

namespace {
void ToProto(const raft_msg::HeartBeatRequest& src, raft::HeartBeatRequest& dst) {
    dst.set_term(src.term);
    dst.set_leader_id(src.leader_id);
    dst.set_prev_index(src.prev_index);
    dst.set_prev_term(src.prev_term);
    dst.set_commited_index(src.commited_index);
    for (const auto& e : src.entries) {
        auto* pe = dst.add_entries();
        pe->set_index(e.index);
        pe->set_term(e.term);
        pe->set_cmd(e.cmd);
    }
}
void FromProto(const raft::HeartBeatReply& src, raft_msg::HeartBeatReply& dst) {
    dst.term = src.term();
    dst.success = src.success();
    dst.next_index = src.next_index();
}
void ToProto(const raft_msg::VoteRequest& src, raft::VoteRequest& dst) {
    dst.set_term(src.term);
    dst.set_candidate_id(src.candidate_id);
    dst.set_last_log_index(src.last_log_index);
    dst.set_last_log_term(src.last_log_term);
}
void FromProto(const raft::VoteReply& src, raft_msg::VoteReply& dst) {
    dst.term = src.term();
    dst.vote_granted = src.vote_granted();
}
void ToProto(const raft_msg::InstallSnapshotRequest& src, raft::InstallSnapshotRequest& dst) {
    dst.set_term(src.term);
    dst.set_leader_id(src.leader_id);
    dst.set_last_snapshot_index(src.last_snapshot_index);
    dst.set_last_snapshot_term(src.last_snapshot_term);
    dst.set_snapshot_data(src.snapshot_data);
}
void FromProto(const raft::InstallSnapshotReply& src, raft_msg::InstallSnapshotReply& dst) {
    dst.term = src.term();
    dst.success = src.success();
}
}  // namespace

bool RaftRPCClient::SendHeartBeat(const raft_msg::HeartBeatRequest &request, raft_msg::HeartBeatReply* response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    raft::HeartBeatRequest proto_req;
    raft::HeartBeatReply proto_resp;
    ToProto(request, proto_req);
    const grpc::Status kStatus = stub_->OnHeartBeat(&context, proto_req, &proto_resp);
    if (!kStatus.ok()) return false;
    FromProto(proto_resp, *response);
    return true;
}

bool RaftRPCClient::SendRequestVote(const raft_msg::VoteRequest &request, raft_msg::VoteReply* response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    raft::VoteRequest proto_req;
    raft::VoteReply proto_resp;
    ToProto(request, proto_req);
    const grpc::Status kStatus = stub_->OnRequestVote(&context, proto_req, &proto_resp);
    if (!kStatus.ok()) return false;
    FromProto(proto_resp, *response);
    return true;
}

bool RaftRPCClient::SendInstallSnapshot(const raft_msg::InstallSnapshotRequest &request, raft_msg::InstallSnapshotReply* response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    raft::InstallSnapshotRequest proto_req;
    raft::InstallSnapshotReply proto_resp;
    ToProto(request, proto_req);
    const grpc::Status kStatus = stub_->OnInstallSnapshot(&context, proto_req, &proto_resp);
    if (!kStatus.ok()) return false;
    FromProto(proto_resp, *response);
    return true;
}

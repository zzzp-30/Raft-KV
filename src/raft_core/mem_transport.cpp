#include "mem_transport.h"
#include "raft_rpc.pb.h"

// ---- Hub ----------------------------------------------------------------

void Hub::RegisterHandler(int id, HeartBeatHandler hb, VoteHandler vote,
                          InstallSnapshotHandler snap) {
    std::lock_guard<std::mutex> lk(mu_);
    slots_[id] = NodeSlot{std::move(hb), std::move(vote), std::move(snap), true};
}

void Hub::Disconnect(int id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = slots_.find(id);
    if (it != slots_.end()) {
        it->second.connected = false;
    }
}

void Hub::Reconnect(int id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = slots_.find(id);
    if (it != slots_.end()) {
        it->second.connected = true;
    }
}

void Hub::SetReliable(bool reliable) {
    reliable_.store(reliable, std::memory_order_relaxed);
}

bool Hub::IsReliable() const {
    return reliable_.load(std::memory_order_relaxed);
}

bool Hub::DeliverHeartBeat(int target_id,
                           const raft::HeartBeatRequest& request,
                           raft::HeartBeatReply* response) {
    HeartBeatHandler handler;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!reliable_.load(std::memory_order_relaxed)) {
            return false;
        }
        auto it = slots_.find(target_id);
        if (it == slots_.end() || !it->second.connected) {
            return false;
        }
        handler = it->second.on_heartbeat;
    }
    // Invoke outside lock — handler may acquire the target RaftNode's mutex.
    handler(&request, response);
    return true;
}

bool Hub::DeliverVote(int target_id,
                      const raft::VoteRequest& request,
                      raft::VoteReply* response) {
    VoteHandler handler;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!reliable_.load(std::memory_order_relaxed)) {
            return false;
        }
        auto it = slots_.find(target_id);
        if (it == slots_.end() || !it->second.connected) {
            return false;
        }
        handler = it->second.on_vote;
    }
    handler(&request, response);
    return true;
}

// ---- MemTransport -------------------------------------------------------

bool MemTransport::SendHeartBeat(const raft::HeartBeatRequest& request,
                                 raft::HeartBeatReply* response) {
    return hub_->DeliverHeartBeat(target_id_, request, response);
}

bool MemTransport::SendRequestVote(const raft::VoteRequest& request,
                                   raft::VoteReply* response) {
    return hub_->DeliverVote(target_id_, request, response);
}

bool Hub::DeliverInstallSnapshot(int target_id,
                                 const raft::InstallSnapshotRequest& request,
                                 raft::InstallSnapshotReply* response) {
    InstallSnapshotHandler handler;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!reliable_.load(std::memory_order_relaxed)) {
            return false;
        }
        auto it = slots_.find(target_id);
        if (it == slots_.end() || !it->second.connected) {
            return false;
        }
        handler = it->second.on_snapshot;
    }
    handler(&request, response);
    return true;
}

bool MemTransport::SendInstallSnapshot(const raft::InstallSnapshotRequest& request,
                                       raft::InstallSnapshotReply* response) {
    return hub_->DeliverInstallSnapshot(target_id_, request, response);
}

#pragma once
#ifndef MEMTRANSPORT_H
#define MEMTRANSPORT_H

#include "i_transport.h"
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace raft {
class HeartBeatRequest;
class HeartBeatReply;
class VoteRequest;
class VoteReply;
class InstallSnapshotRequest;
class InstallSnapshotReply;
}  // namespace raft

// Handler types: type-erased callbacks that dispatch to RaftNode::OnXxx.
// Using std::function avoids a circular dependency between MemTransport and RaftNode.
using HeartBeatHandler = std::function<void(const raft::HeartBeatRequest*,
                                            raft::HeartBeatReply*)>;
using VoteHandler = std::function<void(const raft::VoteRequest*,
                                       raft::VoteReply*)>;
using InstallSnapshotHandler = std::function<void(const raft::InstallSnapshotRequest*,
                                                  raft::InstallSnapshotReply*)>;

// Hub: central message router for in-memory transports.
// Simulates a network switch connecting N Raft nodes.
// Supports fault injection: disconnect/reconnect nodes, drop messages.
class Hub {
private:
    struct NodeSlot {
        HeartBeatHandler on_heartbeat;
        VoteHandler on_vote;
        InstallSnapshotHandler on_snapshot;
        bool connected = true;
    };

    mutable std::mutex mu_;
    std::map<int, NodeSlot> slots_;
    std::atomic<bool> reliable_{true};

public:
    Hub() = default;

    void RegisterHandler(int id, HeartBeatHandler hb, VoteHandler vote,
                         InstallSnapshotHandler snap);

    // Simulate network partition: messages to/from this node are dropped.
    void Disconnect(int id);
    void Reconnect(int id);

    // When false, SendXxx may silently drop messages (returns false).
    void SetReliable(bool reliable);
    bool IsReliable() const;

    // Called by MemTransport. Returns false if target unreachable or message dropped.
    bool DeliverHeartBeat(int target_id,
                          const raft::HeartBeatRequest& request,
                          raft::HeartBeatReply* response);
    bool DeliverVote(int target_id,
                     const raft::VoteRequest& request,
                     raft::VoteReply* response);
    bool DeliverInstallSnapshot(int target_id,
                                const raft::InstallSnapshotRequest& request,
                                raft::InstallSnapshotReply* response);
};

// MemTransport: in-memory ITransport implementation.
// Mirrors RaftRPCClient: each instance is a channel TO a specific target node.
// RaftNode stores transports_[id] → MemTransport(target=id) for each peer.
class MemTransport : public ITransport {
private:
    std::shared_ptr<Hub> hub_;
    int target_id_;

public:
    MemTransport(std::shared_ptr<Hub> hub, int target_id)
        : hub_(std::move(hub)), target_id_(target_id) {}

    bool SendHeartBeat(const raft::HeartBeatRequest& request,
                       raft::HeartBeatReply* response) override;
    bool SendRequestVote(const raft::VoteRequest& request,
                         raft::VoteReply* response) override;
    bool SendInstallSnapshot(const raft::InstallSnapshotRequest& request,
                             raft::InstallSnapshotReply* response) override;
};

#endif  // MEMTRANSPORT_H

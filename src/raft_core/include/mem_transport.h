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

using HeartBeatHandler = std::function<void(const raft_msg::HeartBeatRequest*,
                                            raft_msg::HeartBeatReply*)>;
using VoteHandler = std::function<void(const raft_msg::VoteRequest*,
                                       raft_msg::VoteReply*)>;
using InstallSnapshotHandler = std::function<void(const raft_msg::InstallSnapshotRequest*,
                                                  raft_msg::InstallSnapshotReply*)>;

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

    void Disconnect(int id);
    void Reconnect(int id);

    void SetReliable(bool reliable);
    bool IsReliable() const;

    bool DeliverHeartBeat(int target_id,
                          const raft_msg::HeartBeatRequest& request,
                          raft_msg::HeartBeatReply* response);
    bool DeliverVote(int target_id,
                     const raft_msg::VoteRequest& request,
                     raft_msg::VoteReply* response);
    bool DeliverInstallSnapshot(int target_id,
                                const raft_msg::InstallSnapshotRequest& request,
                                raft_msg::InstallSnapshotReply* response);
};

class MemTransport : public ITransport {
private:
    std::shared_ptr<Hub> hub_;
    int target_id_;

public:
    MemTransport(std::shared_ptr<Hub> hub, int target_id)
        : hub_(std::move(hub)), target_id_(target_id) {}

    bool SendHeartBeat(const raft_msg::HeartBeatRequest& request,
                       raft_msg::HeartBeatReply* response) override;
    bool SendRequestVote(const raft_msg::VoteRequest& request,
                         raft_msg::VoteReply* response) override;
    bool SendInstallSnapshot(const raft_msg::InstallSnapshotRequest& request,
                             raft_msg::InstallSnapshotReply* response) override;
};

#endif  // MEMTRANSPORT_H

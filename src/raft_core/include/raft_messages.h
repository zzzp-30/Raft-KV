#pragma once
#ifndef RAFT_MESSAGES_H
#define RAFT_MESSAGES_H

#include <string>
#include <vector>

namespace raft_msg {

struct LogEntry {
    int32_t index = 0;
    int32_t term = 0;
    std::string cmd;
};

struct HeartBeatRequest {
    int32_t term = 0;
    int32_t leader_id = 0;
    int32_t prev_index = 0;
    int32_t prev_term = 0;
    int32_t commited_index = 0;
    std::vector<LogEntry> entries;
};

struct HeartBeatReply {
    int32_t term = 0;
    bool success = false;
    int32_t next_index = 0;
};

struct VoteRequest {
    int32_t term = 0;
    int32_t candidate_id = 0;
    int32_t last_log_index = 0;
    int32_t last_log_term = 0;
};

struct VoteReply {
    int32_t term = 0;
    bool vote_granted = false;
};

struct InstallSnapshotRequest {
    int32_t term = 0;
    int32_t leader_id = 0;
    int32_t last_snapshot_index = 0;
    int32_t last_snapshot_term = 0;
    std::string snapshot_data;
};

struct InstallSnapshotReply {
    int32_t term = 0;
    bool success = false;
};

}  // namespace raft_msg

#endif  // RAFT_MESSAGES_H

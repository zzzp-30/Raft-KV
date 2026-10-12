#pragma once
#ifndef ITRANSPORT_H
#define ITRANSPORT_H

#include "raft_messages.h"

// Transport abstraction: decouples RaftNode from gRPC.
// In production, RaftRPCClient implements this over gRPC channels.
// In testing, MemTransport implements this with in-memory channels.
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual bool SendHeartBeat(const raft_msg::HeartBeatRequest& request,
                               raft_msg::HeartBeatReply* response) = 0;
    virtual bool SendRequestVote(const raft_msg::VoteRequest& request,
                                 raft_msg::VoteReply* response) = 0;
    virtual bool SendInstallSnapshot(const raft_msg::InstallSnapshotRequest& request,
                                     raft_msg::InstallSnapshotReply* response) = 0;
};

#endif  // ITRANSPORT_H

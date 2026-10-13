#pragma once
#ifndef RAFTINSPECTOR_H
#define RAFTINSPECTOR_H

#include <string>
#include <vector>

// Forward declaration — Entry is defined in raft_node.h.
struct Entry;

// Read-only inspection interface for testing.
// RaftNode implements this so that InvariantChecker can query internal state
// without accessing private members directly.
class RaftInspector {
public:
    virtual ~RaftInspector() = default;
    virtual int InspectorId() const = 0;
    virtual int InspectorTerm() const = 0;
    virtual bool InspectorIsLeader() const = 0;
    virtual bool InspectorIsCandidate() const = 0;
    virtual int InspectorCommitIndex() const = 0;
    virtual int InspectorLastApplied() const = 0;
    virtual int InspectorLastSnapshotIndex() const = 0;
    virtual std::vector<Entry> InspectorLog() const = 0;
};

#endif  // RAFTINSPECTOR_H

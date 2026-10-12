#pragma once
#ifndef INVARIANTCHECKER_H
#define INVARIANTCHECKER_H

#include <string>
#include <vector>

class RaftInspector;

// Result of a single invariant check.
struct Violation {
    std::string rule;    // e.g. "LeaderCompleteness"
    std::string detail;  // human-readable description
};

// InvariantChecker: verifies Raft safety properties across a cluster snapshot.
// All checks are read-only and thread-safe (each inspector call acquires its lock).
//
// Checked invariants:
//   1. Leader Completeness — at most one leader per term
//   2. Log Prefix Consistency — matching (index, term) implies identical preceding logs
class InvariantChecker {
public:
    // Run all invariant checks. Returns empty vector if all hold.
    static std::vector<Violation> CheckAll(
        const std::vector<RaftInspector*>& nodes);

    // Individual checks.
    static std::vector<Violation> CheckLeaderCompleteness(
        const std::vector<RaftInspector*>& nodes);
    static std::vector<Violation> CheckLogConsistency(
        const std::vector<RaftInspector*>& nodes);

    // Convenience: check all and return a single summary string.
    // Empty string means all invariants hold.
    static std::string CheckAllSummary(
        const std::vector<RaftInspector*>& nodes);
};

#endif  // INVARIANTCHECKER_H

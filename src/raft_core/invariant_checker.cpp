#include "invariant_checker.h"
#include "raft_inspector.h"
#include "raft_node.h"  // Entry
#include <map>
#include <sstream>

std::vector<Violation> InvariantChecker::CheckAll(
    const std::vector<RaftInspector*>& nodes) {
    auto violations = CheckLeaderCompleteness(nodes);
    auto log_violations = CheckLogConsistency(nodes);
    violations.insert(violations.end(),
                      log_violations.begin(), log_violations.end());
    return violations;
}

std::vector<Violation> InvariantChecker::CheckLeaderCompleteness(
    const std::vector<RaftInspector*>& nodes) {
    std::vector<Violation> violations;
    std::map<int, std::vector<int>> leaders_by_term;

    for (auto* node : nodes) {
        if (node->InspectorIsLeader()) {
            leaders_by_term[node->InspectorTerm()].push_back(node->InspectorId());
        }
    }

    for (const auto& [term, leaders] : leaders_by_term) {
        if (leaders.size() > 1) {
            std::ostringstream oss;
            oss << "term " << term << " has " << leaders.size()
                << " leaders: [";
            for (size_t i = 0; i < leaders.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << leaders[i];
            }
            oss << "]";
            violations.push_back({"LeaderCompleteness", oss.str()});
        }
    }
    return violations;
}

std::vector<Violation> InvariantChecker::CheckLogConsistency(
    const std::vector<RaftInspector*>& nodes) {
    std::vector<Violation> violations;

    for (size_t i = 0; i < nodes.size(); ++i) {
        auto log_i = nodes[i]->InspectorLog();
        for (size_t j = i + 1; j < nodes.size(); ++j) {
            auto log_j = nodes[j]->InspectorLog();
            size_t min_len = std::min(log_i.size(), log_j.size());
            for (size_t k = 1; k < min_len; ++k) {
                if (log_i[k].index == log_j[k].index &&
                    log_i[k].term == log_j[k].term &&
                    log_i[k].cmd != log_j[k].cmd) {
                    std::ostringstream oss;
                    oss << "node " << nodes[i]->InspectorId()
                        << " vs node " << nodes[j]->InspectorId()
                        << ": entry at index=" << log_i[k].index
                        << " term=" << log_i[k].term
                        << " has different cmd ('"
                        << log_i[k].cmd << "' vs '"
                        << log_j[k].cmd << "')";
                    violations.push_back({"LogPrefixConsistency", oss.str()});
                }
            }
        }
    }
    return violations;
}

std::string InvariantChecker::CheckAllSummary(
    const std::vector<RaftInspector*>& nodes) {
    auto violations = CheckAll(nodes);
    if (violations.empty()) {
        return "OK: all invariants hold";
    }
    std::ostringstream oss;
    oss << "FAIL: " << violations.size() << " violation(s):\n";
    for (const auto& v : violations) {
        oss << "  [" << v.rule << "] " << v.detail << "\n";
    }
    return oss.str();
}

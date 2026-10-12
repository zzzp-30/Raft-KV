#pragma once
#ifndef TESTHARNESS_H
#define TESTHARNESS_H

#include "fault_injector.h"
#include "invariant_checker.h"
#include "mem_persister.h"
#include "mem_transport.h"
#include "raft_node.h"
#include "kv_store.h"
#include <chrono>
#include <memory>
#include <string>
#include <vector>

// Per-node bookkeeping inside the harness.
struct TestNode {
    std::unique_ptr<RaftNode> node;
    std::unique_ptr<KVStore> kv;
    std::shared_ptr<MemPersister> persister;
    std::thread start_thread;   // runs RaftNode::Start() (Ticker blocks)
};

// TestHarness: creates and manages an in-memory Raft cluster for testing.
//
// Usage:
//   TestHarness h(3);                    // 3-node cluster, all started
//   int leader = h.WaitForLeader(10000); // wait up to 10s
//   h.Propose(leader, "put x 42");       // propose through leader
//   h.CheckInvariants();                 // verify safety properties
class TestHarness {
private:
    int node_count_;
    std::shared_ptr<Hub> hub_;
    std::unique_ptr<FaultInjector> faults_;
    std::vector<std::unique_ptr<TestNode>> nodes_;

public:
    // Build a cluster of `n` nodes and start them all.
    explicit TestHarness(int n);
    ~TestHarness();

    TestHarness(const TestHarness&) = delete;
    TestHarness& operator=(const TestHarness&) = delete;

    // Returns leader id, or -1 if no leader within timeout_ms.
    int WaitForLeader(int timeout_ms);

    // Propose a command through the given node (should be leader).
    // Returns true if committed and applied within timeout.
    bool Propose(int node_id, const std::string& cmd, int timeout_ms = 5000);

    // Run all invariant checks. Returns empty string if all hold.
    std::string CheckInvariants();

    // Direct accessors.
    RaftNode* GetNode(int id) { return nodes_[id]->node.get(); }
    KVStore* GetKV(int id) { return nodes_[id]->kv.get(); }
    RaftInspector* GetInspector(int id) { return nodes_[id]->node.get(); }
    FaultInjector& GetFaults() { return *faults_; }
    int NodeCount() const { return node_count_; }

    // Shut down a node, clear its persisted state and KV store, then restart it.
    // The node comes back with an empty log — it must catch up via InstallSnapshot.
    void RestartNode(int id);

    // Collect all inspectors for InvariantChecker.
    std::vector<RaftInspector*> AllInspectors();
};

#endif  // TESTHARNESS_H

#pragma once
#ifndef FAULTINJECTOR_H
#define FAULTINJECTOR_H

#include <memory>
#include <vector>

class Hub;
class RaftInspector;

// FaultInjector: orchestrates network-level fault scenarios for testing.
// Wraps a Hub to provide a clean API for disconnecting/reconnecting nodes
// and toggling message reliability.
//
// Usage (in test harness):
//   auto hub = std::make_shared<Hub>();
//   FaultInjector faults(hub);
//   // ... start cluster ...
//   faults.IsolateNode(1);          // partition node 1
//   faults.SetReliable(false);      // start dropping all messages
//   faults.RestoreAll();            // reconnect everything
class FaultInjector {
private:
    std::shared_ptr<Hub> hub_;
    int node_count_;

public:
    explicit FaultInjector(std::shared_ptr<Hub> hub, int node_count)
        : hub_(std::move(hub)), node_count_(node_count) {}

    // Partition a single node: all messages to/from it are dropped.
    void IsolateNode(int id);

    // Restore a previously isolated node.
    void RestoreNode(int id);

    // Partition two nodes from each other (both directions).
    void PartitionPair(int a, int b);

    // Drop all messages globally.
    void SetUnreliable();
    void SetReliable();

    // Restore all nodes and re-enable reliable delivery.
    void RestoreAll();

    int NodeCount() const { return node_count_; }
};

#endif  // FAULTINJECTOR_H

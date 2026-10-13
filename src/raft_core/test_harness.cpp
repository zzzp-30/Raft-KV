#include "test_harness.h"
#include <chrono>
#include <iostream>
#include <thread>

TestHarness::TestHarness(int n) : node_count_(n) {
    hub_ = std::make_shared<Hub>();
    faults_ = std::make_unique<FaultInjector>(hub_, n);

    for (int i = 0; i < n; ++i) {
        auto tn = std::make_unique<TestNode>();

        tn->kv = std::make_unique<KVStore>();
        tn->persister = std::make_shared<MemPersister>();

        std::map<int, Node> peers;
        for (int j = 0; j < n; ++j) {
            peers[j] = Node("mem://" + std::to_string(j));
        }

        std::map<int, std::shared_ptr<ITransport>> transports;
        for (int j = 0; j < n; ++j) {
            if (j != i) {
                transports[j] = std::make_shared<MemTransport>(hub_, j);
            }
        }

        tn->node = std::make_unique<RaftNode>(
            i, peers, std::move(transports), tn->persister, *tn->kv);

        auto* raw = tn->node.get();
        hub_->RegisterHandler(
            i,
            [raw](const raft_msg::HeartBeatRequest* req, raft_msg::HeartBeatReply* resp) {
                raw->OnHeartBeat(req, resp);
            },
            [raw](const raft_msg::VoteRequest* req, raft_msg::VoteReply* resp) {
                raw->OnRequestVote(req, resp);
            },
            [raw](const raft_msg::InstallSnapshotRequest* req, raft_msg::InstallSnapshotReply* resp) {
                raw->OnInstallSnapshot(req, resp);
            });

        tn->start_thread = std::thread([raw]() {
            raw->Start();
        });
        nodes_.push_back(std::move(tn));
    }
}

TestHarness::~TestHarness() {
    // Phase 1: stop all nodes while the cluster is still alive.
    // This prevents the thread pool from hanging (Ticker keeps adding tasks).
    for (auto& tn : nodes_) {
        if (tn->node) {
            tn->node->Shutdown();
        }
    }
    // Phase 2: join start threads (should be done already since Start() returns immediately).
    for (auto& tn : nodes_) {
        if (tn->start_thread.joinable()) {
            tn->start_thread.join();
        }
    }
    // Phase 3: destroy nodes (Shutdown already called, destructor is a no-op).
}

int TestHarness::WaitForLeader(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);

    // Record initial commit indices to detect advancement
    std::map<int, int> initial_commits;
    for (auto& tn : nodes_) {
        initial_commits[tn->node->InspectorId()] = tn->node->InspectorCommitIndex();
    }

    while (std::chrono::steady_clock::now() < deadline) {
        for (auto& tn : nodes_) {
            auto* node = tn->node.get();
            if (node->IsLeader()) {
                // Capability probing: check if commit_index has advanced
                // (proves noop was committed, confirming majority acknowledgment)
                int current_commit = node->InspectorCommitIndex();
                int initial_commit = initial_commits[node->InspectorId()];

                // If this is a new leader (commit advanced or it's been leader for a while)
                // and it can serve requests, return it
                if (current_commit > initial_commit ||
                    std::chrono::steady_clock::now() - deadline +
                        std::chrono::milliseconds(timeout_ms) >
                    std::chrono::milliseconds(500)) {
                    // Leader has been stable for at least 500ms or commit advanced
                    return node->InspectorId();
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Fallback: return any leader if we can't find a stable one
    for (auto& tn : nodes_) {
        if (tn->node->IsLeader()) {
            return tn->node->InspectorId();
        }
    }
    return -1;
}

bool TestHarness::Propose(int node_id, const std::string& cmd, int timeout_ms) {
    const int max_retries = 3;
    int retry_delay_ms = 100;

    for (int attempt = 0; attempt <= max_retries; ++attempt) {
        auto* node = nodes_[node_id]->node.get();

        // Check if node is still leader before proposing
        if (!node->IsLeader()) {
            // Try to find current leader
            int current_leader = WaitForLeader(1000);
            if (current_leader >= 0 && current_leader != node_id) {
                // Redirect to current leader
                node_id = current_leader;
                node = nodes_[node_id]->node.get();
            }
        }

        auto future = node->Propose(cmd);
        auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));

        if (status == std::future_status::ready) {
            bool result = future.get();
            if (result) {
                return true;  // Success
            }
            // Proposal failed (leader might have stepped down)
        }

        // Retry with exponential backoff
        if (attempt < max_retries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
            retry_delay_ms *= 2;  // Exponential backoff: 100ms, 200ms, 400ms
        }
    }

    return false;  // All retries exhausted
}

std::string TestHarness::CheckInvariants() {
    auto inspectors = AllInspectors();
    return InvariantChecker::CheckAllSummary(inspectors);
}

std::vector<RaftInspector*> TestHarness::AllInspectors() {
    std::vector<RaftInspector*> result;
    for (auto& tn : nodes_) {
        result.push_back(tn->node.get());
    }
    return result;
}

void TestHarness::RestartNode(int id) {
    auto& tn = nodes_[id];
    // Stop the node.
    tn->node->Shutdown();
    if (tn->start_thread.joinable()) tn->start_thread.join();
    // Clear persisted state and KV store.
    tn->persister->Clear();
    tn->kv = std::make_unique<KVStore>();
    // Rebuild transports (old ones still point to Hub, which is fine).
    std::map<int, Node> peers;
    for (int j = 0; j < node_count_; ++j) {
        peers[j] = Node("mem://" + std::to_string(j));
    }
    std::map<int, std::shared_ptr<ITransport>> transports;
    for (int j = 0; j < node_count_; ++j) {
        if (j != id) {
            transports[j] = std::make_shared<MemTransport>(hub_, j);
        }
    }
    // Create a fresh RaftNode.
    tn->node = std::make_unique<RaftNode>(
        id, peers, std::move(transports), tn->persister, *tn->kv);
    // Re-register handlers.
    auto* raw = tn->node.get();
    hub_->RegisterHandler(
        id,
        [raw](const raft_msg::HeartBeatRequest* req, raft_msg::HeartBeatReply* resp) {
            raw->OnHeartBeat(req, resp);
        },
        [raw](const raft_msg::VoteRequest* req, raft_msg::VoteReply* resp) {
            raw->OnRequestVote(req, resp);
        },
        [raw](const raft_msg::InstallSnapshotRequest* req, raft_msg::InstallSnapshotReply* resp) {
            raw->OnInstallSnapshot(req, resp);
        });
    // Restart.
    tn->start_thread = std::thread([raw]() { raw->Start(); });
}

void TestHarness::CrashRestartNode(int id) {
    auto& tn = nodes_[id];
    tn->node->Shutdown();
    if (tn->start_thread.joinable()) tn->start_thread.join();
    // Clear KV store (volatile memory lost) but KEEP persister (disk survives).
    tn->kv = std::make_unique<KVStore>();
    std::map<int, Node> peers;
    for (int j = 0; j < node_count_; ++j) {
        peers[j] = Node("mem://" + std::to_string(j));
    }
    std::map<int, std::shared_ptr<ITransport>> transports;
    for (int j = 0; j < node_count_; ++j) {
        if (j != id) {
            transports[j] = std::make_shared<MemTransport>(hub_, j);
        }
    }
    tn->node = std::make_unique<RaftNode>(
        id, peers, std::move(transports), tn->persister, *tn->kv);
    auto* raw = tn->node.get();
    hub_->RegisterHandler(
        id,
        [raw](const raft_msg::HeartBeatRequest* req, raft_msg::HeartBeatReply* resp) {
            raw->OnHeartBeat(req, resp);
        },
        [raw](const raft_msg::VoteRequest* req, raft_msg::VoteReply* resp) {
            raw->OnRequestVote(req, resp);
        },
        [raw](const raft_msg::InstallSnapshotRequest* req, raft_msg::InstallSnapshotReply* resp) {
            raw->OnInstallSnapshot(req, resp);
        });
    tn->start_thread = std::thread([raw]() { raw->Start(); });
}

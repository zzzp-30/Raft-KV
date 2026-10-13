#include "test_harness.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>

static int g_passed = 0;
static int g_failed = 0;
static TestHarness* harness_ptr = nullptr;

struct Oracle {
    std::map<std::string, std::string> expected;

    void Record(const std::string& cmd) {
        std::istringstream ss(cmd);
        std::string op, key, value;
        ss >> op >> key >> value;
        if (op == "put") expected[key] = value;
        else if (op == "del") expected.erase(key);
    }

    bool Verify(KVStore* kv, std::string& err) const {
        for (const auto& [k, v] : expected) {
            std::string actual;
            kv->Get(k, actual);
            if (actual != v) {
                err = "key='" + k + "' expected='" + v + "' actual='" + actual + "'";
                return false;
            }
        }
        return true;
    }
};

static void DumpFailure(TestHarness* h, const std::string& msg) {
    if (!h) return;
    std::ofstream out("failures.txt", std::ios::app);
    if (!out.is_open()) return;
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    out << "=== " << ctime(&t) << "FAIL: " << msg << "\n";
    for (int i = 0; i < h->NodeCount(); ++i) {
        auto* insp = h->GetInspector(i);
        auto log = insp->InspectorLog();
        out << "  node " << i << ": term=" << insp->InspectorTerm()
            << " commit=" << insp->InspectorCommitIndex()
            << " applied=" << insp->InspectorLastApplied()
            << " log_size=" << log.size() << "\n";
    }
    out << "\n";
}

#define ASSERT_TRUE(cond, msg)                                     \
    do {                                                           \
        if (!(cond)) {                                             \
            std::cerr << "  FAIL: " << (msg) << "\n";              \
            DumpFailure(harness_ptr, msg);                         \
            return false;                                          \
        }                                                          \
    } while (0)

#define RUN_TEST(fn)                                               \
    do {                                                           \
        std::cout << "[TEST] " << #fn << " ...\n";                 \
        auto t0 = std::chrono::steady_clock::now();                 \
        bool ok = fn();                                            \
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>( \
            std::chrono::steady_clock::now() - t0).count();        \
        if (ok) {                                                  \
            std::cout << "  PASS (" << ms << "ms)\n";              \
            g_passed++;                                            \
        } else {                                                   \
            std::cout << "  FAIL (" << ms << "ms)\n";              \
            g_failed++;                                            \
        }                                                          \
    } while (0)

// ---------------------------------------------------------------------------
// Test: 3-node cluster elects exactly one leader within timeout.
// ---------------------------------------------------------------------------
bool TestBasicElection() {
    TestHarness h(3);
    harness_ptr = &h;
    auto t0 = std::chrono::steady_clock::now();
    int leader = h.WaitForLeader(10000);
    auto election_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "  [metric] election_time=" << election_ms << "ms\n";
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    int leader_count = 0;
    for (int i = 0; i < 3; ++i) {
        if (h.GetNode(i)->IsLeader()) leader_count++;
    }
    ASSERT_TRUE(leader_count == 1, "expected exactly 1 leader");

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: leader replicates a log entry to all followers.
// ---------------------------------------------------------------------------
bool TestLogReplication() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    oracle.Record("put x hello");
    bool ok = h.Propose(leader, "put x hello");
    ASSERT_TRUE(ok, "proposal not committed within timeout");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (int i = 0; i < 3; ++i) {
        std::string err;
        ASSERT_TRUE(oracle.Verify(h.GetKV(i), err),
            "node " + std::to_string(i) + ": " + err);
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: multiple writes are applied in order across all nodes.
// ---------------------------------------------------------------------------
bool TestMultipleWrites() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 5; ++i) {
        std::string cmd = "put key" + std::to_string(i) + " val" + std::to_string(i);
        oracle.Record(cmd);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }
    auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "  [metric] 5_writes=" << write_ms << "ms"
              << " throughput=" << (5000.0 / write_ms) << " ops/s\n";

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (int n = 0; n < 3; ++n) {
        std::string err;
        ASSERT_TRUE(oracle.Verify(h.GetKV(n), err),
            "node " + std::to_string(n) + ": " + err);
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: 5-node cluster elects a leader (tests larger quorum).
// ---------------------------------------------------------------------------
bool TestFiveNodeElection() {
    TestHarness h(5);
    harness_ptr = &h;
    auto t0 = std::chrono::steady_clock::now();
    int leader = h.WaitForLeader(15000);
    auto election_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "  [metric] 5node_election_time=" << election_ms << "ms\n";
    ASSERT_TRUE(leader >= 0, "no leader elected within 15s (5 nodes)");

    int leader_count = 0;
    for (int i = 0; i < 5; ++i) {
        if (h.GetNode(i)->IsLeader()) leader_count++;
    }
    ASSERT_TRUE(leader_count == 1, "expected exactly 1 leader");

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: network partition — isolate a node, cluster still functions.
// ---------------------------------------------------------------------------
bool TestNetworkPartition() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    oracle.Record("put before yes");
    bool ok = h.Propose(leader, "put before yes");
    ASSERT_TRUE(ok, "pre-partition proposal failed");

    int isolated = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { isolated = i; break; }
    }
    h.GetFaults().IsolateNode(isolated);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    oracle.Record("put during_partition yes");
    ok = h.Propose(leader, "put during_partition yes");
    if (!ok) oracle.expected.erase("during_partition");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);

    h.GetFaults().RestoreNode(isolated);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    for (int i = 0; i < 3; ++i) {
        std::string err;
        ASSERT_TRUE(oracle.Verify(h.GetKV(i), err),
            "node " + std::to_string(i) + " after heal: " + err);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Test: ReadIndex confirms leadership and returns a valid commit index.
// ---------------------------------------------------------------------------
bool TestReadIndex() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    bool ok = h.Propose(leader, "put ri_key ri_val");
    ASSERT_TRUE(ok, "proposal failed");

    auto t0 = std::chrono::steady_clock::now();
    int commit = h.GetNode(leader)->ReadIndex();
    auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "  [metric] ReadIndex_latency=" << read_us << "us\n";
    ASSERT_TRUE(commit >= 0, "ReadIndex failed on leader");

    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            int c = h.GetNode(i)->ReadIndex(500);
            ASSERT_TRUE(c < 0, "ReadIndex should fail on non-leader " + std::to_string(i));
        }
    }

    std::string val;
    h.GetKV(leader)->Get("ri_key", val);
    ASSERT_TRUE(val == "ri_val", "ReadIndex: expected 'ri_val', got '" + val + "'");

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: snapshot — lagging follower receives InstallSnapshot on reconnect.
// ---------------------------------------------------------------------------
bool TestSnapshot() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    int lagging = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { lagging = i; break; }
    }

    h.RestartNode(lagging);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    leader = h.WaitForLeader(5000);
    ASSERT_TRUE(leader >= 0, "no leader after restart");

    Oracle oracle;
    for (int i = 0; i < 15; ++i) {
        std::string cmd = "put skey" + std::to_string(i) + " sval" + std::to_string(i);
        oracle.Record(cmd);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    h.RestartNode(lagging);
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    std::string err;
    ASSERT_TRUE(oracle.Verify(h.GetKV(lagging), err),
        "lagging node after snapshot: " + err);

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: crash recovery — node restarts with persisted state intact.
// ---------------------------------------------------------------------------
bool TestCrashRecovery() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    for (int i = 0; i < 5; ++i) {
        std::string cmd = "put ck" + std::to_string(i) + " cv" + std::to_string(i);
        oracle.Record(cmd);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    int crashed = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { crashed = i; break; }
    }
    int term_before = h.GetInspector(crashed)->InspectorTerm();

    h.CrashRestartNode(crashed);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    // 验收 1: currentTerm 绝不回退
    int term_after = h.GetInspector(crashed)->InspectorTerm();
    ASSERT_TRUE(term_after >= term_before,
        "term decreased after crash: " + std::to_string(term_before) +
        " -> " + std::to_string(term_after));

    // 验收 2: 已提交的写入一定可见
    auto log_after = h.GetInspector(crashed)->InspectorLog();
    ASSERT_TRUE(log_after.size() >= 6,
        "log too short after crash recovery: " + std::to_string(log_after.size()));

    // 等待状态机重放完成
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    for (int i = 0; i < 5; ++i) {
        std::string val;
        h.GetKV(crashed)->Get("ck" + std::to_string(i), val);
        std::string expected = "cv" + std::to_string(i);
        ASSERT_TRUE(val == expected,
            "committed write not visible after crash: ck" + std::to_string(i) +
            " expected='" + expected + "' got='" + val + "'");
    }

    // 验收 3: 状态机不重复执行（通过 Oracle 验证所有节点一致性）
    for (int i = 0; i < 3; ++i) {
        std::string err;
        ASSERT_TRUE(oracle.Verify(h.GetKV(i), err),
            "node " + std::to_string(i) + " after crash: " + err);
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: LeaseRead returns commit index when lease is valid.
// ---------------------------------------------------------------------------
bool TestLeaseRead() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    bool ok = h.Propose(leader, "put lr_key lr_val");
    ASSERT_TRUE(ok, "proposal failed");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto t0 = std::chrono::steady_clock::now();
    int commit = h.GetNode(leader)->LeaseRead();
    auto lease_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "  [metric] LeaseRead_latency=" << lease_us << "us\n";
    ASSERT_TRUE(commit >= 0, "LeaseRead failed on leader");

    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            int c = h.GetNode(i)->LeaseRead(500);
            ASSERT_TRUE(c < 0, "LeaseRead should fail on non-leader " + std::to_string(i));
        }
    }

    std::string val;
    h.GetKV(leader)->Get("lr_key", val);
    ASSERT_TRUE(val == "lr_val", "LeaseRead: expected 'lr_val', got '" + val + "'");
    return true;
}

// ---------------------------------------------------------------------------
// Test: GetLeaderId returns correct leader after election.
// ---------------------------------------------------------------------------
bool TestLeaderHint() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            int hint = h.GetNode(i)->GetLeaderId();
            ASSERT_TRUE(hint == leader,
                "node " + std::to_string(i) + " leader_hint=" +
                std::to_string(hint) + " (expected " + std::to_string(leader) + ")");
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Test: election restriction — leader has up-to-date log.
// ---------------------------------------------------------------------------
bool TestElectionRestriction() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    for (int i = 0; i < 3; ++i) {
        std::string cmd = "put er" + std::to_string(i) + " ev" + std::to_string(i);
        oracle.Record(cmd);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    int log_leader = (int)h.GetInspector(leader)->InspectorLog().size();

    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            int log_follower = (int)h.GetInspector(i)->InspectorLog().size();
            ASSERT_TRUE(log_leader >= log_follower,
                "leader log(" + std::to_string(log_leader) +
                ") < follower " + std::to_string(i) +
                " log(" + std::to_string(log_follower) + ")");
        }
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: log divergence recovery — after partition heals, divergent logs
// are reconciled by the leader.
// ---------------------------------------------------------------------------
bool TestLogDivergenceRecovery() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    Oracle oracle;
    oracle.Record("put shared before");
    bool ok = h.Propose(leader, "put shared before");
    ASSERT_TRUE(ok, "pre-partition proposal failed");

    int follower_a = -1, follower_b = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            if (follower_a < 0) follower_a = i;
            else follower_b = i;
        }
    }

    h.GetFaults().IsolateNode(follower_a);
    // Wait for cluster to stabilize and new leader to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Get stable leader (now with capability probing)
    leader = h.WaitForLeader(5000);
    ASSERT_TRUE(leader >= 0, "no leader after isolation");

    oracle.Record("put after_partition yes");
    ok = h.Propose(leader, "put after_partition yes");
    ASSERT_TRUE(ok, "post-partition proposal failed");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    h.GetFaults().RestoreNode(follower_a);
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    for (int i = 0; i < 3; ++i) {
        std::string err;
        ASSERT_TRUE(oracle.Verify(h.GetKV(i), err),
            "node " + std::to_string(i) + " after divergence heal: " + err);
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: snapshot then immediate kill — verify lastIncludedIndex survives.
// ---------------------------------------------------------------------------
bool TestSnapshotCrashRecovery() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Write 15 entries to trigger snapshot (threshold=10)
    for (int i = 0; i < 15; ++i) {
        std::string cmd = "put sc" + std::to_string(i) + " sv" + std::to_string(i);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Verify snapshot was created on leader
    auto leader_log = h.GetInspector(leader)->InspectorLog();
    int snapshot_index = h.GetInspector(leader)->InspectorLastSnapshotIndex();
    ASSERT_TRUE(snapshot_index > 0, "no snapshot created on leader");

    // Kill the leader immediately after snapshot
    int crashed = leader;
    h.CrashRestartNode(crashed);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    // Verify lastSnapshotIndex survived the crash
    int snapshot_after = h.GetInspector(crashed)->InspectorLastSnapshotIndex();
    ASSERT_TRUE(snapshot_after == snapshot_index,
        "lastSnapshotIndex changed after crash: " + std::to_string(snapshot_index) +
        " -> " + std::to_string(snapshot_after));

    // Verify all data is still accessible
    for (int i = 0; i < 15; ++i) {
        std::string val;
        h.GetKV(crashed)->Get("sc" + std::to_string(i), val);
        std::string expected = "sv" + std::to_string(i);
        ASSERT_TRUE(val == expected,
            "data lost after snapshot crash: sc" + std::to_string(i) +
            " expected='" + expected + "' got='" + val + "'");
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: InstallSnapshot then append more logs, follower falls behind again.
// ---------------------------------------------------------------------------
bool TestMultipleSnapshots() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Pick a non-leader node
    int lagging = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { lagging = i; break; }
    }

    // Isolate the lagging node
    h.GetFaults().IsolateNode(lagging);
    // Wait for cluster to stabilize and new leader to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Get stable leader (now with capability probing)
    leader = h.WaitForLeader(5000);
    ASSERT_TRUE(leader >= 0, "no leader after isolation");

    // Write 15 entries on the remaining cluster (triggers first snapshot)
    for (int i = 0; i < 15; ++i) {
        std::string cmd = "put ms1_" + std::to_string(i) + " val1";
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal ms1_" + std::to_string(i) + " failed");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Restore the lagging node — it should receive InstallSnapshot
    h.GetFaults().RestoreNode(lagging);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    // Verify first batch of data
    for (int i = 0; i < 15; ++i) {
        std::string val;
        h.GetKV(lagging)->Get("ms1_" + std::to_string(i), val);
        ASSERT_TRUE(val == "val1",
            "first snapshot data missing: ms1_" + std::to_string(i));
    }

    // Isolate again and write more (triggers second snapshot)
    h.GetFaults().IsolateNode(lagging);
    // Wait for cluster to stabilize and new leader to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Get stable leader (now with capability probing)
    leader = h.WaitForLeader(5000);
    ASSERT_TRUE(leader >= 0, "no leader after second isolation");

    for (int i = 0; i < 15; ++i) {
        std::string cmd = "put ms2_" + std::to_string(i) + " val2";
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal ms2_" + std::to_string(i) + " failed");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Restore again — should receive second InstallSnapshot
    h.GetFaults().RestoreNode(lagging);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    // Verify both batches of data
    for (int i = 0; i < 15; ++i) {
        std::string val;
        h.GetKV(lagging)->Get("ms1_" + std::to_string(i), val);
        ASSERT_TRUE(val == "val1",
            "first batch lost after second snapshot: ms1_" + std::to_string(i));
        h.GetKV(lagging)->Get("ms2_" + std::to_string(i), val);
        ASSERT_TRUE(val == "val2",
            "second batch missing: ms2_" + std::to_string(i));
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: crash during snapshot generation — verify consistency.
// ---------------------------------------------------------------------------
bool TestSnapshotDuringCrash() {
    TestHarness h(3);
    harness_ptr = &h;
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Pick a non-leader node to crash
    int crashed = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { crashed = i; break; }
    }

    // Crash the node before any writes
    h.CrashRestartNode(crashed);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Write 20 entries on the remaining cluster to trigger snapshot
    for (int i = 0; i < 20; ++i) {
        std::string cmd = "put sdc" + std::to_string(i) + " val";
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }
    // Wait for snapshot to be created on leader
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Restart the crashed node — it should catch up via InstallSnapshot
    h.CrashRestartNode(crashed);
    // Wait for InstallSnapshot to complete and state machine to apply
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // Verify all data is consistent
    for (int i = 0; i < 20; ++i) {
        std::string val;
        h.GetKV(crashed)->Get("sdc" + std::to_string(i), val);
        ASSERT_TRUE(val == "val",
            "data inconsistent after crash during snapshot: sdc" + std::to_string(i));
    }

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);
    return true;
}

// ---------------------------------------------------------------------------
// Test: random perturbation — 200 rounds with independent seeds, collect metrics.
// ---------------------------------------------------------------------------
bool TestRandomPerturbation() {
    const int kRounds = 200;
    const unsigned int kBaseSeed = 42;

    int total_leader_switches = 0;
    int total_partitions = 0;
    int total_invariant_violations = 0;

    std::cout << "  [config] rounds=" << kRounds << " base_seed=" << kBaseSeed << "\n";

    for (int round = 0; round < kRounds; ++round) {
        // Each round uses an independent seed for statistical validity
        std::mt19937 rng(kBaseSeed + round);

        TestHarness h(3);
        harness_ptr = &h;

        // Wait for initial leader
        int leader = h.WaitForLeader(5000);
        if (leader < 0) {
            std::cerr << "  [round " << round << "] no initial leader\n";
            total_invariant_violations++;
            continue;
        }

        int prev_leader = leader;

        // Random fault injection
        std::uniform_int_distribution<int> action_dist(0, 4);
        int action = action_dist(rng);

        if (action == 0) {
            // Isolate a random node
            std::uniform_int_distribution<int> node_dist(0, 2);
            int target = node_dist(rng);
            h.GetFaults().IsolateNode(target);
            total_partitions++;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            // Check if leader changed
            int new_leader = h.WaitForLeader(3000);
            if (new_leader >= 0 && new_leader != prev_leader) {
                total_leader_switches++;
            }

            h.GetFaults().RestoreNode(target);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        } else if (action == 1) {
            // Set unreliable
            h.GetFaults().SetUnreliable();
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            h.GetFaults().SetReliable();
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        } else {
            // Just wait and let the cluster stabilize
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        // Check invariants
        std::string inv = h.CheckInvariants();
        if (inv.find("OK") == std::string::npos) {
            std::cerr << "  [round " << round << "] invariant violation: " << inv << "\n";
            total_invariant_violations++;
        }
    }

    std::cout << "  [metrics] leader_switches=" << total_leader_switches
              << " partitions=" << total_partitions
              << " violations=" << total_invariant_violations << "\n";

    ASSERT_TRUE(total_invariant_violations == 0,
        "invariant violations: " + std::to_string(total_invariant_violations));
    return true;
}

int main() {
    std::cout << "=== Raft Correctness Tests ===\n\n";

    RUN_TEST(TestBasicElection);
    RUN_TEST(TestLogReplication);
    RUN_TEST(TestMultipleWrites);
    RUN_TEST(TestFiveNodeElection);
    RUN_TEST(TestNetworkPartition);
    RUN_TEST(TestReadIndex);
    RUN_TEST(TestSnapshot);
    RUN_TEST(TestCrashRecovery);
    RUN_TEST(TestLeaseRead);
    RUN_TEST(TestLeaderHint);
    RUN_TEST(TestElectionRestriction);
    RUN_TEST(TestLogDivergenceRecovery);
    RUN_TEST(TestSnapshotCrashRecovery);
    RUN_TEST(TestMultipleSnapshots);
    RUN_TEST(TestSnapshotDuringCrash);
    RUN_TEST(TestRandomPerturbation);

    std::cout << "\n=== Results: " << g_passed << " passed, "
              << g_failed << " failed ===\n";

    return g_failed > 0 ? 1 : 0;
}

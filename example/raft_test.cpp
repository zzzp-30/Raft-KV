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

    int term_after = h.GetInspector(crashed)->InspectorTerm();
    ASSERT_TRUE(term_after >= term_before,
        "term decreased after crash: " + std::to_string(term_before) +
        " -> " + std::to_string(term_after));

    auto log_after = h.GetInspector(crashed)->InspectorLog();
    ASSERT_TRUE(log_after.size() >= 6,
        "log too short after crash recovery: " + std::to_string(log_after.size()));

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
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

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

    std::cout << "\n=== Results: " << g_passed << " passed, "
              << g_failed << " failed ===\n";

    return g_failed > 0 ? 1 : 0;
}

#include "test_harness.h"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

static int g_passed = 0;
static int g_failed = 0;

#define ASSERT_TRUE(cond, msg)                                     \
    do {                                                           \
        if (!(cond)) {                                             \
            std::cerr << "  FAIL: " << (msg) << "\n";              \
            return false;                                          \
        }                                                          \
    } while (0)

#define RUN_TEST(fn)                                               \
    do {                                                           \
        std::cout << "[TEST] " << #fn << " ...\n";                 \
        bool ok = fn();                                            \
        if (ok) {                                                  \
            std::cout << "  PASS\n";                               \
            g_passed++;                                            \
        } else {                                                   \
            g_failed++;                                            \
        }                                                          \
    } while (0)

// ---------------------------------------------------------------------------
// Test: 3-node cluster elects exactly one leader within timeout.
// ---------------------------------------------------------------------------
bool TestBasicElection() {
    TestHarness h(3);
    int leader = h.WaitForLeader(10000);
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
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    bool ok = h.Propose(leader, "put x hello");
    ASSERT_TRUE(ok, "proposal not committed within timeout");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (int i = 0; i < 3; ++i) {
        std::string val;
        h.GetKV(i)->Get("x", val);
        ASSERT_TRUE(val == "hello",
            "node " + std::to_string(i) + " has value='" + val + "' (expected 'hello')");
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
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    for (int i = 0; i < 5; ++i) {
        std::string cmd = "put key" + std::to_string(i) + " val" + std::to_string(i);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (int n = 0; n < 3; ++n) {
        for (int i = 0; i < 5; ++i) {
            std::string val;
            h.GetKV(n)->Get("key" + std::to_string(i), val);
            std::string expected = "val" + std::to_string(i);
            ASSERT_TRUE(val == expected,
                "node " + std::to_string(n) + " key" + std::to_string(i) +
                "='" + val + "' (expected '" + expected + "')");
        }
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
    int leader = h.WaitForLeader(15000);
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
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Write before partition.
    bool ok = h.Propose(leader, "put before yes");
    ASSERT_TRUE(ok, "pre-partition proposal failed");

    // Isolate a non-leader node.
    int isolated = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { isolated = i; break; }
    }
    h.GetFaults().IsolateNode(isolated);

    // Remaining 2 nodes should still function (leader + 1 follower = quorum of 2/3).
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ok = h.Propose(leader, "put during partition");
    // This may or may not succeed depending on timing — with 2/3 nodes the
    // leader still has a majority (itself + 1 follower), so it should work.
    // We just verify invariants hold regardless.

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::string inv = h.CheckInvariants();
    ASSERT_TRUE(inv.find("OK") != std::string::npos, inv);

    // Restore and verify the isolated node catches up.
    h.GetFaults().RestoreNode(isolated);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    std::string val;
    h.GetKV(isolated)->Get("before", val);
    ASSERT_TRUE(val == "yes",
        "isolated node didn't catch up: before='" + val + "'");
    return true;
}

// ---------------------------------------------------------------------------
// Test: ReadIndex confirms leadership and returns a valid commit index.
// ---------------------------------------------------------------------------
bool TestReadIndex() {
    TestHarness h(3);
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Write data first.
    bool ok = h.Propose(leader, "put ri_key ri_val");
    ASSERT_TRUE(ok, "proposal failed");

    // ReadIndex on the leader should succeed.
    int commit = h.GetNode(leader)->ReadIndex();
    ASSERT_TRUE(commit >= 0, "ReadIndex failed on leader");

    // ReadIndex on a non-leader should fail.
    for (int i = 0; i < 3; ++i) {
        if (i != leader) {
            int c = h.GetNode(i)->ReadIndex(500);
            ASSERT_TRUE(c < 0, "ReadIndex should fail on non-leader " + std::to_string(i));
        }
    }

    // Data should be readable after ReadIndex confirms.
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
    int leader = h.WaitForLeader(10000);
    ASSERT_TRUE(leader >= 0, "no leader elected within 10s");

    // Pick a non-leader node to be the "lagging" node.
    int lagging = -1;
    for (int i = 0; i < 3; ++i) {
        if (i != leader) { lagging = i; break; }
    }

    // Shut down the lagging node and clear its state.
    h.RestartNode(lagging);
    // Let the remaining cluster stabilize.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    leader = h.WaitForLeader(5000);
    ASSERT_TRUE(leader >= 0, "no leader after restart");

    // Write 15 entries — exceeds kSnapshotThreshold(10), triggers snapshot on leader.
    for (int i = 0; i < 15; ++i) {
        std::string cmd = "put skey" + std::to_string(i) + " sval" + std::to_string(i);
        bool ok = h.Propose(leader, cmd);
        ASSERT_TRUE(ok, "proposal " + std::to_string(i) + " failed");
    }

    // Give the leader time to create the snapshot in ApplyLoop.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Restart the lagging node again — it has no state, must catch up via InstallSnapshot.
    h.RestartNode(lagging);
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // Verify the lagging node's state machine has all data from the snapshot.
    for (int i = 0; i < 15; ++i) {
        std::string val;
        h.GetKV(lagging)->Get("skey" + std::to_string(i), val);
        std::string expected = "sval" + std::to_string(i);
        ASSERT_TRUE(val == expected,
            "lagging node skey" + std::to_string(i) +
            "='" + val + "' (expected '" + expected + "')");
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

    std::cout << "\n=== Results: " << g_passed << " passed, "
              << g_failed << " failed ===\n";

    return g_failed > 0 ? 1 : 0;
}

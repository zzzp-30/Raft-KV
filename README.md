# kv-storage-based-on-Raft

## 1. 项目简介

本项目是基于 Raft 共识算法实现的分布式高可用 KV 存储系统，使用 C++17 开发。节点之间通过 gRPC 完成选举、心跳、日志复制和快照安装，使用 Protobuf 定义通信协议，并通过本地文件持久化 Raft 状态与日志。

项目包含完整的**确定性正确性测试框架**（参考 MIT 6.824 config.go 模式），可在内存环境中模拟网络分区、节点故障等场景，自动化验证 Raft 算法的安全性不变量。Raft 核心通过接口抽象与 gRPC 完全解耦，可脱离 gRPC 独立编译。

### 技术栈

| 技术 | 用途 |
|---|---|
| C++17 | 项目主要开发语言 |
| CMake + Ninja | 跨平台构建与模块依赖管理 |
| gRPC | 节点间 RPC、客户端请求通信（可选，核心不依赖） |
| Protocol Buffers | RPC 服务与消息序列化 |
| Raft | Leader 选举、日志复制、提交与应用 |
| 接口抽象 | ITransport / IPersister / IStateMachine / IClock 解耦核心与基础设施 |
| 内部消息结构 | raft_msg:: 命名空间定义纯 C++ 消息类型，替代 proto 直接依赖 |
| 内存传输层 | MemTransport + Hub 实现零网络开销的测试集群 |
| 故障注入 | FaultInjector 模拟网络分区与消息丢失 |
| 不变量检查 | InvariantChecker 验证 Leader 完备性与日志前缀一致性 |
| ReadIndex | 线性一致读：心跳多数确认 + 等待状态机 apply |
| Lease Read | 基于心跳租约的本地读，无需额外 RTT |
| Log Snapshot | 日志快照 + InstallSnapshot RPC 实现日志压缩 |
| Thread Pool | 通用线程池模块，用于并发 RPC 发送 |
| 文件持久化 | 保存 `currentTerm`、`votedFor`、Raft 日志和快照 |
| KV 状态机 | 基于内存 `std::map` 实现键值存储 |

### 核心模块

| 模块 | 说明 |
|---|---|
| `raft_core` | Raft 节点状态机、选举、心跳、日志复制、快照、ReadIndex、LeaseRead |
| `raft_core/testing` | 测试框架：MemTransport、FaultInjector、InvariantChecker、TestHarness、Oracle |
| `raft_proto_msg` | Protobuf 消息类（纯序列化，无 gRPC 依赖） |
| `raft_proto` | gRPC 服务桩代码（依赖 raft_proto_msg + gRPC） |
| `server_proto` | 客户端与服务端通信协议 |
| `kv_store` | 内存 KV 状态机，支持 put / get / del + 快照序列化/恢复 |
| `storage` | Raft 与 KV 状态机之间的适配层，封装 ReadIndex / LeaseRead |
| `server` | 节点启动、gRPC 服务注册和客户端请求处理 |
| `client` | 客户端连接、Leader 发现和命令交互 |
| `common` | 线程池等通用基础设施 |

### 技术特点

**Raft 核心**
- Leader 选举、心跳维持、日志复制与提交
- ReadIndex 线性一致读：心跳多数确认 -> 等待状态机 apply -> 返回 commit_index
- Lease Read：Leader 通过心跳多数确认维护租约，租约内直接返回 commit_index，无需额外 RTT
- leader_hint：每个节点跟踪已知 Leader ID，非 Leader 节点可返回重定向目标
- 日志快照：状态机序列化 -> 持久化 -> 日志压缩 -> InstallSnapshot RPC 同步落后节点
- 多线程安全：互斥锁 + 条件变量 + atomic + Future/Promise

**架构设计**
- 四层接口抽象（ITransport / IPersister / IStateMachine / IClock）解耦 Raft 核心与基础设施
- 内部消息结构（`raft_msg::`）替代 proto 直接依赖，raft_core 可脱离 gRPC 独立编译（`BUILD_RAFT_GRPC=OFF`）
- gRPC 传输层作为桥接：负责 `raft_msg::` <-> `raft::` 类型转换
- 生产环境使用 gRPC + 文件持久化，测试环境使用 MemTransport + MemPersister
- Hub 中心化消息路由，支持网络分区模拟和消息丢弃

**正确性测试框架**
- TestHarness：一行代码创建 N 节点内存集群
- FaultInjector：IsolateNode / RestoreNode / SetUnreliable
- InvariantChecker：Leader 完备性（每任期至多一个 Leader）+ 日志前缀一致性
- Oracle：独立写入追踪器，对每个节点 KVStore 逐一验证状态一致性
- CrashRestartNode：保留持久化状态重启，验证崩溃恢复正确性
- 失败自动转储：断言失败时记录全部节点状态到 failures.txt
- 12 个自动化测试 + 可量化性能指标输出

## 2. 实现效果

### 核心亮点

**1. gRPC 解耦 — 核心算法可独立编译**

定义内部 C++ 消息结构体（`raft_msg::` 命名空间），替代 raft_core 中对 Protobuf 生成类型的直接依赖。将 `raft_proto` 拆分为 `raft_proto_msg`（纯 Protobuf 序列化）和 `raft_proto`（gRPC 服务桩）。通过 CMake 选项 `BUILD_RAFT_GRPC=OFF` 可验证 raft_core 在不链接 gRPC 的情况下完整编译——证明 Raft 核心仅依赖抽象接口，不绑定任何具体通信框架。

**2. Lease Read + leader_hint 重定向**

- Lease Read：Leader 在每次心跳获得多数确认后延长租约（`kLeaseDurationMs=300ms`），`LeaseRead()` 在租约有效期内直接返回 `commit_index`，无需心跳往返。实测延迟 1~3us（vs ReadIndex 的 ~5000us），提升约 3 个数量级
- leader_hint：每个节点通过心跳跟踪已知 Leader（`leader_id_`），`GetLeaderId()` 供客户端在非 Leader 节点上获取重定向目标
- 引入 IClock 抽象接口，支持生产环境 SteadyClock 和测试环境手动时钟注入

**3. 崩溃恢复测试**

`CrashRestartNode()` 关闭节点但保留持久化状态（模拟磁盘存活），清空 KV 状态机（模拟内存丢失）。`TestCrashRecovery` 验证：
- 任期单调不降（term monotonicity）
- 崩溃前已提交的日志条目在恢复后完整保留
- 恢复后不变量仍然成立

**4. 独立 Oracle + 失败现场转储**

- Oracle：独立于 Raft 的写入追踪器，每次成功的 Propose 记录到 expected 映射，测试结束时对每个节点的 KVStore 逐一验证
- 失败转储：断言失败时自动将全部节点的 term、commit index、applied index、log size 写入 failures.txt

**5. 12 个自动化测试 + 可量化指标**

| 测试 | 验证内容 |
|---|---|
| TestBasicElection | 3 节点选出唯一 Leader（含选举耗时指标） |
| TestLogReplication | Leader 复制日志到所有 Follower（Oracle 验证） |
| TestMultipleWrites | 5 次顺序写入，全节点一致（含吞吐量指标） |
| TestFiveNodeElection | 5 节点集群选举（更大 quorum） |
| TestNetworkPartition | 网络分区下集群可用性 + 恢复后 Oracle 全节点验证 |
| TestReadIndex | 线性一致读（含延迟指标） |
| TestSnapshot | 快照创建 + InstallSnapshot 恢复落后节点 |
| TestCrashRecovery | 崩溃后任期单调性 + 日志完整性 |
| TestLeaseRead | 租约读（含延迟指标） |
| TestLeaderHint | Follower 正确报告 Leader ID |
| TestElectionRestriction | Leader 日志长度 >= 所有 Follower |
| TestLogDivergenceRecovery | 分区愈合后分歧日志自动收敛 |

### 实测指标（可复现）

| 指标 | 数值 |
|---|---|
| 3 节点选举耗时 | ~800-900ms |
| 5 节点选举耗时 | ~600ms |
| 写入吞吐量（串行 5 次 Propose） | ~5 ops/s |
| ReadIndex 延迟 | ~5000us |
| LeaseRead 延迟 | ~1-3us |

### 技术架构

```text
客户端 --> gRPC --> Server --> RaftStorage --> RaftNode (核心)
                                              |-- ITransport (gRPC / MemTransport)
                                              |-- IPersister (文件 / MemPersister)
                                              |-- IStateMachine (KVStore)
                                              |-- IClock (SteadyClock / 测试手动时钟)
```

raft_core 仅依赖 raft_proto_msg（Protobuf 消息类）和抽象接口，不直接依赖 gRPC。gRPC 传输实现（raft_rpc_client.cpp / raft_rpc.cpp）作为桥接层负责 raft_msg:: 与 raft:: 类型之间的转换。

## 3. 运行方法

### 运行环境

* 操作系统：Linux / Windows（本项目在 Ubuntu 24.04 和 Windows 11 上验证）
* C++ 标准：C++17
* 构建工具：CMake 3.16 及以上
* 依赖库：gRPC C++、Protocol Buffers、Threads

### 编译

```bash
cd kv-storage-raft
# 默认构建（含 gRPC 传输层）
cmake -B build -G Ninja -DCMAKE_TOOLCHAIN_FILE=<vcpkg>/scripts/buildsystems/vcpkg.cmake
cmake --build build

# 仅核心（不含 gRPC，验证接口解耦）
cmake -B build -G Ninja -DBUILD_RAFT_GRPC=OFF -DCMAKE_TOOLCHAIN_FILE=<vcpkg>/scripts/buildsystems/vcpkg.cmake
cmake --build build
```

### 运行分布式集群

本项目提供了 3 节点 + 客户端的示例：

* 在 3 个终端分别运行节点：
  ```bash
  ./run/1start.sh
  ./run/2start.sh
  ./run/3start.sh
  ```

* 运行客户端程序：
  ```bash
  ./bin/client_test
  ```
  支持命令：`put key value` / `get key` / `del key`

### 运行正确性测试

```bash
./bin/raft_test
```

输出示例：

```
=== Raft Correctness Tests ===

[TEST] TestBasicElection ...
  [metric] election_time=805ms
  PASS (1207ms)
[TEST] TestReadIndex ...
  [metric] ReadIndex_latency=5260us
  PASS (1203ms)
[TEST] TestLeaseRead ...
  [metric] LeaseRead_latency=3us
  PASS (2208ms)
...
=== Results: 12 passed, 0 failed ===
```

## 4. 项目目录树

```text
Raft/
├── CMakeLists.txt                     // 顶层构建，含 BUILD_RAFT_GRPC 选项
├── bin/                               // 构建产物目录
├── build/                             // CMake 构建目录
├── lib/                               // 静态库目录
├── example/                           // 示例与测试程序
│   ├── main.cpp                       // 分布式集群入口
│   ├── client_test.cpp                // 客户端测试
│   ├── server_test.cpp                // 服务端测试
│   ├── raft_test.cpp                  // 正确性测试（12 个用例 + 指标输出）
│   └── thread_pool_test.cpp
├── run/                               // 启动脚本
│   ├── 1start.sh / 2start.sh / 3start.sh
└── src/
    ├── common/                        // 线程池
    ├── raft_proto/                    // Raft RPC 协议定义
    │   ├── CMakeLists.txt             // 拆分 raft_proto_msg + raft_proto
    │   └── raft_rpc.proto             // HeartBeat / RequestVote / InstallSnapshot
    ├── server_proto/                  // 客户端-服务端 RPC 协议
    ├── raft_core/                     // Raft 核心
    │   ├── include/
    │   │   ├── raft_node.h            // Raft 节点（选举/日志/快照/ReadIndex/LeaseRead）
    │   │   ├── raft_messages.h        // 内部消息结构体（raft_msg::）
    │   │   ├── i_clock.h              // 时钟抽象接口 + SteadyClock
    │   │   ├── raft_rpc.h             // gRPC 服务端（桥接层）
    │   │   ├── raft_rpc_client.h      // gRPC 客户端（桥接层）
    │   │   ├── persister.h            // 文件持久化
    │   │   ├── i_transport.h          // 传输层接口
    │   │   ├── i_persister.h          // 持久化接口
    │   │   ├── mem_transport.h        // 内存传输层 + Hub
    │   │   ├── mem_persister.h        // 内存持久化
    │   │   ├── raft_inspector.h       // 只读检查接口（测试用）
    │   │   ├── fault_injector.h       // 故障注入
    │   │   ├── invariant_checker.h    // 不变量检查
    │   │   └── test_harness.h         // 测试集群管理 + CrashRestartNode
    │   ├── raft_node.cpp
    │   ├── raft_rpc.cpp               // proto <-> raft_msg 转换（仅 gRPC 模式编译）
    │   ├── raft_rpc_client.cpp        // proto <-> raft_msg 转换（仅 gRPC 模式编译）
    │   ├── persister.cpp
    │   ├── mem_transport.cpp
    │   ├── fault_injector.cpp
    │   ├── invariant_checker.cpp
    │   └── test_harness.cpp
    ├── kv_store/                      // KV 状态机
    │   ├── include/
    │   │   ├── i_state_machine.h      // 状态机接口（含 CreateSnapshot/ApplySnapshot）
    │   │   └── kv_store.h
    │   └── kv_store.cpp
    ├── storage/                       // Raft + KV 适配层
    ├── server/                        // 服务端
    └── client/                        // 客户端
```

# kv-storage-based-on-Raft

## 1. 项目简介

本项目是基于 Raft 共识算法实现的分布式高可用 KV 存储系统，使用 C++17 开发。节点之间通过 gRPC 完成选举、心跳、日志复制和快照安装，使用 Protobuf 定义通信协议，并通过本地文件持久化 Raft 状态与日志。

项目包含完整的**确定性正确性测试框架**（参考 MIT 6.824 config.go 模式），可在内存环境中模拟网络分区、节点故障等场景，自动化验证 Raft 算法的安全性不变量。

### 技术栈

| 技术 | 用途 |
|---|---|
| C++17 | 项目主要开发语言 |
| CMake + Ninja | 跨平台构建与模块依赖管理 |
| gRPC | 节点间 RPC、客户端请求通信 |
| Protocol Buffers | RPC 服务与消息序列化 |
| Raft | Leader 选举、日志复制、提交与应用 |
| 接口抽象 | ITransport / IPersister / IStateMachine 解耦核心与基础设施 |
| 内存传输层 | MemTransport + Hub 实现零网络开销的测试集群 |
| 故障注入 | FaultInjector 模拟网络分区与消息丢失 |
| 不变量检查 | InvariantChecker 验证 Leader 完备性与日志前缀一致性 |
| ReadIndex | 线性一致读：心跳多数确认 + 等待状态机 apply |
| Log Snapshot | 日志快照 + InstallSnapshot RPC 实现日志压缩 |
| Thread Pool | 通用线程池模块，用于并发 RPC 发送 |
| 文件持久化 | 保存 `currentTerm`、`votedFor`、Raft 日志和快照 |
| KV 状态机 | 基于内存 `std::map` 实现键值存储 |

### 核心模块

| 模块 | 说明 |
|---|---|
| `raft_core` | Raft 节点状态机、选举、心跳、日志复制、快照、持久化 |
| `raft_core/testing` | 测试框架：MemTransport、FaultInjector、InvariantChecker、TestHarness |
| `raft_proto` | Raft 节点间通信协议（HeartBeat、RequestVote、InstallSnapshot） |
| `server_proto` | 客户端与服务端通信协议 |
| `kv_store` | 内存 KV 状态机，支持 put / get / del + 快照序列化/恢复 |
| `storage` | Raft 与 KV 状态机之间的适配层，封装 ReadIndex 线性一致读 |
| `server` | 节点启动、gRPC 服务注册和客户端请求处理 |
| `client` | 客户端连接、Leader 发现和命令交互 |
| `common` | 线程池等通用基础设施 |

### 技术特点

**Raft 核心**
- Leader 选举、心跳维持、日志复制与提交
- ReadIndex 线性一致读：心跳多数确认 → 等待状态机 apply → 返回 commit_index
- 日志快照：状态机序列化 → 持久化 → 日志压缩 → InstallSnapshot RPC 同步落后节点
- 多线程安全：互斥锁 + 条件变量 + atomic + Future/Promise

**架构设计**
- 接口抽象（ITransport / IPersister / IStateMachine）解耦 Raft 核心与基础设施
- 生产环境使用 gRPC + 文件持久化，测试环境使用 MemTransport + MemPersister
- Hub 中心化消息路由，支持网络分区模拟和消息丢弃

**正确性测试框架**
- TestHarness：一行代码创建 N 节点内存集群
- FaultInjector：IsolateNode / RestoreNode / SetUnreliable
- InvariantChecker：Leader 完备性（每任期至多一个 Leader）+ 日志前缀一致性
- 7 个自动化测试覆盖：选举、日志复制、多写、5 节点集群、网络分区、线性一致读、快照恢复

## 2. 运行方法

### 运行环境

* 操作系统：Linux / Windows（本项目在 Ubuntu 24.04 和 Windows 11 上验证）
* C++ 标准：C++17
* 构建工具：CMake 3.16 及以上
* 依赖库：gRPC C++、Protocol Buffers、Threads

### 编译

```bash
cd kv-storage-raft
# Linux
./build.sh
# Windows
do_build.bat
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

测试用例：

| 测试 | 验证内容 |
|---|---|
| TestBasicElection | 3 节点集群选出唯一 Leader |
| TestLogReplication | Leader 复制日志到所有 Follower |
| TestMultipleWrites | 多次写入按序应用到所有节点 |
| TestFiveNodeElection | 5 节点集群选举（更大 quorum） |
| TestNetworkPartition | 网络分区下集群可用性 + 恢复后数据一致 |
| TestReadIndex | 线性一致读在 Leader 上成功、非 Leader 上失败 |
| TestSnapshot | 快照创建 + InstallSnapshot 恢复落后节点 |

## 3. 项目目录树

```text
Raft/
├── CMakeLists.txt
├── build.sh / do_build.bat       // 构建脚本
├── bin/                           // 构建产物目录
├── build/                         // CMake 构建目录
├── lib/                           // 静态库目录
├── example/                       // 示例与测试程序
│   ├── main.cpp                   // 分布式集群入口
│   ├── client_test.cpp            // 客户端测试
│   ├── server_test.cpp            // 服务端测试
│   ├── raft_test.cpp              // 正确性测试（7 个用例）
│   └── thread_pool_test.cpp
├── run/                           // 启动脚本
│   ├── 1start.sh / 2start.sh / 3start.sh
└── src/
    ├── common/                    // 线程池
    ├── raft_proto/                // Raft RPC 协议定义
    │   └── raft_rpc.proto         // HeartBeat / RequestVote / InstallSnapshot
    ├── server_proto/              // 客户端-服务端 RPC 协议
    ├── raft_core/                 // Raft 核心
    │   ├── include/
    │   │   ├── raft_node.h        // Raft 节点（选举/日志/快照/ReadIndex）
    │   │   ├── raft_rpc.h         // gRPC 服务端
    │   │   ├── raft_rpc_client.h  // gRPC 客户端
    │   │   ├── persister.h        // 文件持久化
    │   │   ├── i_transport.h      // 传输层接口
    │   │   ├── i_persister.h      // 持久化接口
    │   │   ├── mem_transport.h    // 内存传输层 + Hub
    │   │   ├── mem_persister.h    // 内存持久化
    │   │   ├── raft_inspector.h   // 只读检查接口（测试用）
    │   │   ├── fault_injector.h   // 故障注入
    │   │   ├── invariant_checker.h // 不变量检查
    │   │   └── test_harness.h     // 测试集群管理
    │   ├── raft_node.cpp
    │   ├── raft_rpc.cpp / raft_rpc_client.cpp
    │   ├── persister.cpp
    │   ├── mem_transport.cpp
    │   ├── fault_injector.cpp
    │   ├── invariant_checker.cpp
    │   └── test_harness.cpp
    ├── kv_store/                  // KV 状态机
    │   ├── include/
    │   │   ├── i_state_machine.h  // 状态机接口
    │   │   └── kv_store.h
    │   └── kv_store.cpp
    ├── storage/                   // Raft + KV 适配层
    ├── server/                    // 服务端
    └── client/                    // 客户端
```

#pragma once
#include <thread>
#ifndef RAFT_H
#define RAFT_H
#include <future>
#include <memory>
#include <random>
#include <utility>
#include <condition_variable>
#include <map>
#include<string>
#include <mutex>
#include <atomic>
#ifdef _WIN32
#include <process.h>
#endif
#include "i_state_machine.h"
#include "raft_messages.h"
#include "i_transport.h"
#include "i_persister.h"
#include "i_clock.h"
#include "raft_inspector.h"
#include"thread_pool.h"
//节点基本信息
struct Node{
    bool is_connect;
    std::string address;
    Node(){};
    Node(const std::string&address):address(address){
        is_connect = true;
    }
};
typedef std::string CMD;
//日志记录结构
struct Entry
{
    int index;  //序号
    int term;   //任期
    CMD cmd;    //命令
};
//raft节点定义
class RaftNode : public RaftInspector{
private:
    using MyPromise = std::promise<bool>;
    using MyFuture = std::future<bool>;
private:
    //节点状态枚举
    enum class State{
        kFollower=0,
        kCandidate,
        kLeader
    };
private:
//通信相关
    //除本节点外其他节点信息
    std::map<int,Node> peers_;
    //传输层(每个peer一个)
    std::map<int,std::shared_ptr<ITransport>> transports_;
//选举信息相关
    //本节点id
    int me_;    
    //本节点状态 
    State state_;
    //当前任期   
    int current_term_;   
    //当前获得票数
    int vote_count_;
    //本轮投票所给节点id, 若没投票则为-1
    int vote_for_;  
//定时资源
    mutable std::mutex mu_;
    std::condition_variable cond_;
    std::condition_variable apply_cv_;  // notified when entries are applied to state machine
    //超时时间点
    std::chrono::steady_clock::time_point deadline_;    

    //随机数发生器
    std::mt19937_64 rng_;

//日志相关
    //日志容器,序号从1开始
    std::vector<Entry> log_; 
    //上次提交序号
    int commited_index_;
    //上次应用到状态机序号
    int last_applied_;
    //保存发送给每个节点的下一条记录序号
    std::map<int,int>next_indexs_;
    //保存已经复制给每个节点的最后一条记录序号
    std::map<int,int>match_indexs_;
    //快照点:最后快照的日志序号和任期
    int last_snapshot_index_ = 0;
    int last_snapshot_term_ = 0;
    static constexpr int kSnapshotThreshold = 10;
// Lease read
    std::shared_ptr<IClock> clock_;
    std::chrono::steady_clock::time_point lease_expire_;
    int heartbeat_confirms_ = 0;
    static constexpr int kLeaseDurationMs = 300;
    int leader_id_ = -1;
//并发资源
    std::unique_ptr<std::thread> ticker_thread_;
    std::unique_ptr<std::thread> thread_apply_loop_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::atomic<bool> running_{true};
private:
    std::shared_ptr<IPersister> persister_;  //持久化模块
private:
    std::map<std::pair<int,int>,std::shared_ptr<std::promise<bool>>> pendding_map_;   //外部请求等待
    IStateMachine& state_machine_;
private:
    //初始化随机数发生器
    std::mt19937_64 MakeRng(int node_id) {
    std::random_device rd;

    std::seed_seq seed{
        rd(),
        rd(),
        rd(),
        rd(),
        static_cast<unsigned int>(node_id),
#ifdef _WIN32
        static_cast<unsigned int>(::_getpid()),
#else
        static_cast<unsigned int>(::getpid()),
#endif
    };

    return std::mt19937_64(seed);
    }
//日志容器函数
private:
    //日志相关状态初始化
    void LogInit();
    //将Raft日志序号映射到log_数组下标(快照压缩后log_[0]为sentinel)
    int LogPos(int raft_index) const { return raft_index - last_snapshot_index_; }
    int  GetLastIndex()
    {
        if(log_.size()<=1)
        {
            return last_snapshot_index_;
        }
        return log_.back().index;
    }
    int GetLastTerm()
    {
        if(log_.size() <= 1)
        {
            return last_snapshot_term_;
        }
        return log_.back().term;
    }
    //传入日志更新或一致,返回true
    bool NewerOrEqualLogs(int term,int index){
        if(this->GetLastTerm() < term)
            return true;
        else if(this->GetLastTerm() > term)
            return false;
        if(this->GetLastIndex() > index)
            return false;
        return true;
    }
    //模拟客户端定期追加日志
    void CliLike();
//持久化函数
private:
    //保存current_term_和vote_for
    bool SaveState();
    //加载current_term_和vote_for
    bool LoadState();   
    //保存日志
    bool SaveLog();
    //加载日志
    bool LoadLog();
    //加载快照(启动时恢复状态机+sentinel日志)
    bool LoadSnapshot();
    //创建快照:序列化状态机,持久化,压缩日志
    void MaybeCreateSnapshot();
    //提交日志
    bool CommitLog(int index);
    //检查,更新commit_index
    bool UpdateCommit();
//主干函数 和 RPC请求封装
private:
    void FollowerRun();
    void CandidateRun();
    void LeaderRun();
    void BroadcastRequestVote();
    void SendRequestVote(int id,const int kTermSnapshot);
    void BroadcastHeartBeat();
    void SendHeartBeat(int id,const int kTermSnapshot);
    void SendSnapshot(int id, const int kTermSnapshot);
private:
    // void resetHeartBeatTimer();
    //应用日志到状态机
    void ApplyLoop();
//RPC响应封装
public:
    void OnHeartBeat(const raft_msg::HeartBeatRequest* request,raft_msg::HeartBeatReply* response);
    void OnRequestVote(const raft_msg::VoteRequest* request,raft_msg::VoteReply* response);
    void OnInstallSnapshot(const raft_msg::InstallSnapshotRequest* request,raft_msg::InstallSnapshotReply* response);
public:
    RaftNode(int id,
             const std::map<int,Node>& kNodes,
             std::map<int,std::shared_ptr<ITransport>> transports,
             std::shared_ptr<IPersister> persister,
             IStateMachine& kv_store,
             std::shared_ptr<IClock> clock = std::make_shared<SteadyClock>())
    : me_(id)
    , peers_(kNodes)
    , transports_(std::move(transports))
    , rng_(MakeRng(id))
    , persister_(std::move(persister))
    , state_machine_(kv_store)
    , clock_(std::move(clock)) {
        peers_.erase(id);
        transports_.erase(id);
        this->log_.push_back(Entry{0,0,""});
        thread_pool_ = std::make_unique<ThreadPool>(3);
    }
    ~RaftNode(){
        // If Shutdown() wasn't called explicitly, do it now.
        if(running_.load(std::memory_order_relaxed))
            Shutdown();
    }
    //开启raft节点
    void Start();
    //优雅关闭: 停止 Ticker, 排空线程池, 停止 ApplyLoop
    void Shutdown() {
        running_.store(false, std::memory_order_relaxed);
        cond_.notify_all();
        if(ticker_thread_ && ticker_thread_->joinable())
            ticker_thread_->join();
        thread_pool_.reset();
        if(thread_apply_loop_ && thread_apply_loop_->joinable())
            thread_apply_loop_->join();
    }
private:
    //定时器函数
    void Ticker();
    //重置选举定时器
    void ResetElectionTimerLocked();
    //重新心跳定时器
    void ResetHeartBeatTimerLocked();
    //选举超时行为
    void OnElectionTimeout();
    //心跳超时行为
    void OnHeartBeatTimeout();

public:
    //外部调用,请求添加日志
    MyFuture Propose(const std::string&cmd);

public:
    //Server调用,公开leader状态
    bool IsLeader()const
    {
        std::lock_guard<std::mutex> lck(mu_);
        return this->state_ == State::kLeader;
    }
    //ReadIndex: 确认leader身份(心跳多数确认), 等待状态机追上commit, 返回commit_index
    //return: 成功确认返回commit_index(>=0), 失败返回-1
    int ReadIndex(int timeout_ms = 1000);
    //LeaseRead: 若leader租约未过期,直接返回commit_index;否则降级为ReadIndex
    int LeaseRead(int timeout_ms = 1000);
    //GetLeaderId: 返回已知leader id, 未知返回-1
    int GetLeaderId() const
    {
        std::lock_guard<std::mutex> lck(mu_);
        return leader_id_;
    }

// RaftInspector implementation (read-only access for testing)
public:
    int InspectorId() const override { return me_; }
    int InspectorTerm() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return current_term_;
    }
    bool InspectorIsLeader() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return state_ == State::kLeader;
    }
    bool InspectorIsCandidate() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return state_ == State::kCandidate;
    }
    int InspectorCommitIndex() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return commited_index_;
    }
    int InspectorLastApplied() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return last_applied_;
    }
    std::vector<Entry> InspectorLog() const override
    {
        std::lock_guard<std::mutex> lck(mu_);
        return log_;
    }
};


#endif //!RAFT_H

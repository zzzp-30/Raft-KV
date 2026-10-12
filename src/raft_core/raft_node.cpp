#include"raft_node.h"
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include<thread>
#include <tuple>
#include <utility>
void RaftNode::Start() {
    {
        std::lock_guard<std::mutex> lck1(mu_);
        //初始化节点信息
        state_ = State::kFollower;
        current_term_ = 0;
        vote_count_ = 0;
        vote_for_ = -1;
        commited_index_ = 0;
        last_applied_ = 0;
        //加载持久化信息
        LoadSnapshot();
        LoadState();
        LoadLog();
        //状态机应用
        thread_apply_loop_ = std::make_unique<std::thread>(&RaftNode::ApplyLoop,this); 
    }
    //启动定时线程
    ticker_thread_ = std::make_unique<std::thread>(&RaftNode::Ticker,this);
}
void RaftNode::OnHeartBeat(const raft_msg::HeartBeatRequest* request,raft_msg::HeartBeatReply* response) {
    
    std::unique_lock<std::mutex> lck1(mu_);
    if(this->current_term_ < request->term)
    {
        this->current_term_ = request->term;
        this->vote_for_ = -1;
        this->state_ = State::kFollower;
        this->leader_id_ = request->leader_id;
        SaveState();
    }else 
    if(this->current_term_> request->term)
    {
        response->term = this->current_term_;
        response->success = false;
        return;
    }
    response->term = this->current_term_;
    this->state_ = State::kFollower;
    this->leader_id_ = request->leader_id;

    ResetElectionTimerLocked();
    
    int prev_index = request->prev_index;
    
    if(prev_index < last_snapshot_index_)
    {
        response->success = false;
        return;
    }
    if(this->GetLastIndex() < prev_index || this->log_[LogPos(prev_index)].term != request->prev_term)
    {
        response->success = false;
    }else
    {
        if(!request->entries.empty())
        {
            int pos = LogPos(prev_index) + 1;
            if(pos > 0 && pos < (int)this->log_.size())
                this->log_.erase(this->log_.begin()+pos,this->log_.end());
            else if(pos <= 0)
                std::cerr << "[ERROR]:log index overflow";
            for(const auto& e : request->entries)
            {
                this->log_.emplace_back(Entry{e.index, e.term, e.cmd});
            }
            SaveLog();
            std::cout << "append ok, log:" << std::endl;
            for(const auto& kS : log_)
            {
                std::cout << "(" << kS.index << ',' << kS.term <<')' << kS.cmd << std::endl;
            }
        }
        if(commited_index_ < request->commited_index)
        {
            commited_index_ = std::min(GetLastIndex(),request->commited_index);
        }
        response->success = true;
    }
}
//广播要票请求
void RaftNode::BroadcastRequestVote() {

    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,node]:peers_)
    {
        thread_pool_->AddTask([this,id = id,term_snapshot](){
            SendRequestVote(id,term_snapshot);
        });
    }
}
void RaftNode::SendRequestVote(int id,const int kTermSnapshot)
{
    raft_msg::VoteRequest request;
    raft_msg::VoteReply response;
    std::unique_lock<std::mutex> lck(mu_);
    if(!(state_ == State::kCandidate && this->current_term_ == kTermSnapshot))
    {
        return;
    }
    request.candidate_id = this->me_;
    request.term = this->current_term_;
    request.last_log_term = GetLastTerm();
    request.last_log_index = GetLastIndex();
    lck.unlock();
    bool res = transports_[id]->SendRequestVote(request,&response);
    if(!res)    return;
    lck.lock();
    if(this->current_term_ < response.term)
    {
        this->current_term_ = response.term;
        vote_for_ = -1;
        state_ = State::kFollower;
        leader_id_ = -1;
        SaveState();
        ResetElectionTimerLocked();
        return;
    }
    if(!(state_ == State::kCandidate && this->current_term_ == kTermSnapshot))
    {
        return;
    }
 
    if(response.vote_granted && this->state_ == State::kCandidate && response.term == request.term)
    {
        this->vote_count_++;
        if(this->vote_count_>=(this->peers_.size()+1)/2+1)
        {
            std::cout<< me_ << " is leader" << std::endl; 
            this->state_ = State::kLeader;
            this->leader_id_ = this->me_;
            this->lease_expire_ = clock_->Now();
            this->heartbeat_confirms_ = 0;
            this->log_.emplace_back(Entry{GetLastIndex()+1,this->current_term_,"no-op"});
            LogInit();
            ResetHeartBeatTimerLocked();
        }
    }
}

//广播心跳包
void RaftNode::BroadcastHeartBeat() {
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,_] : peers_)
    {
        thread_pool_->AddTask([this,id = id,term_snapshot](){
            SendHeartBeat(id,term_snapshot);
        });
    }
}
void RaftNode::SendHeartBeat(int id,const int kTermSnapshot)
{
    raft_msg::HeartBeatRequest request;
    raft_msg::HeartBeatReply response;
    std::unique_lock<std::mutex>lck1(mu_);
    if(!(this->state_ == State::kLeader && this->current_term_ ==kTermSnapshot))
    {
        return;
    }
    if(this->next_indexs_[id] <= this->last_snapshot_index_)
    {
        lck1.unlock();
        SendSnapshot(id, kTermSnapshot);
        return;
    }
    request.leader_id = me_;
    request.term = current_term_;
    request.commited_index = this->commited_index_;
    int prev_index = this->next_indexs_[id]-1;
    request.prev_index = prev_index;
    request.prev_term = this->log_[LogPos(prev_index)].term;

    if(GetLastIndex() >= this->next_indexs_[id])
    {
        for(auto it = this->log_.begin()+LogPos(prev_index)+1;it!=this->log_.end();it++)
        {
            request.entries.push_back(raft_msg::LogEntry{it->index, it->term, it->cmd});
        }
    }

    lck1.unlock();
    bool res = transports_[id]->SendHeartBeat(request,&response);
    if(!res)    return;
    lck1.lock();
    if(!(this->state_ == State::kLeader && this->current_term_ ==kTermSnapshot))
    {
        return;
    }

    if(this->current_term_ < response.term)
    {
        this->current_term_ = response.term;
        vote_for_ = -1;
        state_ = State::kFollower;
        leader_id_ = -1;
        SaveState();
        ResetElectionTimerLocked();
        return;
    }

    if(response.success)
    {
        if(!request.entries.empty())
        {
            match_indexs_[id] = next_indexs_[id] + (int)request.entries.size()-1;
            next_indexs_[id] = match_indexs_[id] +1;
            UpdateCommit();
        }
        heartbeat_confirms_++;
        int majority = static_cast<int>((peers_.size() + 1) / 2) + 1;
        if (heartbeat_confirms_ >= majority) {
            lease_expire_ = clock_->Now() + std::chrono::milliseconds(kLeaseDurationMs);
            heartbeat_confirms_ = 0;
        }
    }else
    {
        next_indexs_[id] --;
    }

    
}
    


void RaftNode::OnRequestVote(const raft_msg::VoteRequest* request,raft_msg::VoteReply* response) {
    std::lock_guard<std::mutex> lck(mu_);
    bool granted = false;
    if(this->current_term_ < request->term)
    {
        this->current_term_ = request->term;
        this->vote_for_ = -1;
        this->state_ = State::kFollower;
        SaveState();
        ResetElectionTimerLocked();
    }
    if(this->current_term_ == request->term && this->vote_for_ ==-1 && NewerOrEqualLogs(request->last_log_term,request->last_log_index))
    {
        granted = true;
    }else{
        granted = false;
    }
    if(granted)
    {
        this->vote_for_ = request->candidate_id;
        SaveState();
        response->vote_granted = true;
        response->term = this->current_term_;
        ResetElectionTimerLocked();
        printf("term=%d:vote for %d\n",this->current_term_,this->vote_for_);
    }else
    {
        response->vote_granted = false;
        response->term = this->current_term_;
    }
}
void RaftNode::LogInit() {
    for(const auto&[id,v]:peers_)
    {
        next_indexs_[id] = GetLastIndex()+1;
        match_indexs_[id] = 0;
    }
}
void RaftNode::CliLike() {
    std::thread th([this](){
        int i = this->GetLastIndex();
        while(this->state_==State::kLeader)
        {
            i++;
            this->log_.emplace_back(Entry{i,this->current_term_,"test_"+std::to_string(i)});
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    });
    th.detach();
}

//必须外部持有锁
bool RaftNode::SaveState() {
    std::string data;
    data = std::to_string(current_term_) + ":" + std::to_string(vote_for_);
    return persister_->SaveRaftState(data);
}
bool RaftNode::LoadState() {
    std::string data;
    bool res = persister_->ReadRaftState(data);
    if(res == false)
        return false;
    auto it = data.find(":");
    this->current_term_ = std::stoi(data.substr(0,it));
    this->vote_for_ = std::stoi(data.substr(it+1));
    printf("loadState :%d:%d\n",current_term_,vote_for_);
    return true;
}
//必须外部持有锁
bool RaftNode::SaveLog() {

    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<size;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persister_->SaveRaftLog(ss.str());
}
bool RaftNode::LoadLog() {
    std::string data;
    persister_->ReadRaftLog(data);
    std::stringstream ss(std::move(data));
    std::string line;
    while(std::getline(ss,line))
    {
        auto it1 = line.find(':');
        auto it2 = line.find(':',it1+1);
        int index = std::stoi(line.substr(0,it1));
        int term = std::stoi(line.substr(it1+1,it2-it1-1));
        std::string cmd = line.substr(it2+1);
        this->log_.emplace_back(Entry{index,term,std::move(cmd)});
        printf("loadLog:%d:%d:%s\n",index,term,cmd.c_str());
    }
    return true;
}
bool RaftNode::CommitLog(int index)
{
    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<=index;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persister_->SaveRaftLog(ss.str());
}
bool RaftNode::UpdateCommit()
{
    int left = commited_index_ +1;
    int right = GetLastIndex();
    int mid;
    int count;
    if(left >right)
    {
        return false;
    }

    while(left <=right)
    {
        mid = (left+right)/2;
        count = 1;
        for(const auto [id,match] : match_indexs_)
        {
            if(mid <=match)
                count++;
        }
        if(count >= (peers_.size()+1)/2+1)
        {
            left = mid + 1;
        }else {
            right = mid-1;
        }
    }
    int to_commit_index;
    if(count >= (peers_.size()+1)/2+1)
    {
        to_commit_index = mid;
    }else{
        to_commit_index = mid-1;
    }   
    if(to_commit_index!=commited_index_ && to_commit_index > last_snapshot_index_
       && this->log_[LogPos(to_commit_index)].term == this->current_term_){
        commited_index_ = to_commit_index;
    }

    return true;
}
RaftNode::MyFuture RaftNode::Propose(const std::string&cmd)
{
    std::lock_guard<std::mutex> lck(mu_);
    if(this->state_ !=State::kLeader)
    {
            MyPromise p;
            p.set_value(false);
            return p.get_future();
    }
    int index = GetLastIndex()+1;
    int term = this->current_term_;
    this->log_.emplace_back(Entry{index,term,cmd});
    this->SaveLog();
    auto p = std::make_shared<MyPromise>();
    this->pendding_map_[std::pair<int, int>(index,term)] = p;
    return p-> get_future();
}
void RaftNode::ApplyLoop()
{
    while(running_.load(std::memory_order_relaxed))
    {
        {
            std::lock_guard<std::mutex> lck(mu_);
            while(last_applied_ < commited_index_)
            {
                int i = last_applied_+1;
                if(i <= last_snapshot_index_) {
                    last_applied_ = last_snapshot_index_;
                    continue;
                }
                Entry &e = this->log_[LogPos(i)];
                std::istringstream ss(e.cmd);
                std::string op;
                std::string key;
                std::string value;
                ss >> op >> key >> value;
                bool success = false;
                if(op == "put")
                {
                    state_machine_.Put(key,value);
                    std::cout << "[APPLY]:" << e.cmd << std::endl;
                    success = true;
                }
                else if(op == "del")
                {
                    state_machine_.Del(key);
                    std::cout << "[APPLY]:" << e.cmd << std::endl;
                    success = true;
                }
                auto index_for_pendding = std::make_pair(e.index,e.term);
                if(this->pendding_map_.find(index_for_pendding) !=pendding_map_.end())
                {
                    auto p = this->pendding_map_[std::make_pair(e.index,e.term)];
                    p->set_value(success);
                    this->pendding_map_.erase(std::make_pair(e.index,e.term));
                }
                last_applied_++;
                apply_cv_.notify_all();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        MaybeCreateSnapshot();
    }
}

void RaftNode::Ticker() {
    {
        std::lock_guard<std::mutex> lck(mu_);
        if(this->state_ == State::kLeader)
        {
            ResetHeartBeatTimerLocked();
        }else{
            ResetElectionTimerLocked();
        }
    }
    std::unique_lock<std::mutex> lck(mu_);
    while(running_.load(std::memory_order_relaxed))
    {
        cond_.wait_until(lck,deadline_);
        if(!running_.load(std::memory_order_relaxed)) break;
        auto now = std::chrono::steady_clock::now();
        if(now>=deadline_)
        {
            switch (state_) {
                case State::kFollower:
                case State::kCandidate:
                    lck.unlock();
                    OnElectionTimeout();
                    lck.lock();
                    ResetElectionTimerLocked();
                    break;
                case State::kLeader:
                    lck.unlock();
                    OnHeartBeatTimeout();
                    lck.lock();
                    ResetHeartBeatTimerLocked();
                    break;
            }
        }else{
            continue;
        }
    }
}
void RaftNode::ResetElectionTimerLocked() {

    auto timeout = std::chrono::milliseconds(500+rng_()%500);

    if(this->state_ == State::kCandidate)
    {
        timeout +=  std::chrono::milliseconds(1000);
    }        
    deadline_ = std::chrono::steady_clock::now() + timeout;
    cond_.notify_all();
}
void RaftNode::ResetHeartBeatTimerLocked() {
    if(state_ == State::kLeader)
    {
        auto timeout = std::chrono::milliseconds(50);
        deadline_ = std::chrono::steady_clock::now()+timeout;
        cond_.notify_all();
    }
}
void RaftNode::OnElectionTimeout() {
    {
        std::lock_guard<std::mutex> lck(mu_);
        current_term_ ++;
        vote_count_ =1;
        vote_for_ = me_;
        this->state_ = State::kCandidate;
        SaveState();
        printf("term=%d:start election\n",this->current_term_);
    }
    BroadcastRequestVote();
}
void RaftNode::OnHeartBeatTimeout() {
    BroadcastHeartBeat();
}

int RaftNode::ReadIndex(int timeout_ms) {
    int recorded_commit;
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_);
        if (state_ != State::kLeader) return -1;
        term_snapshot = current_term_;
        recorded_commit = commited_index_;
    }
    std::atomic<int> confirmations{1};
    std::atomic<bool> lost{false};
    for (const auto& [id, _] : peers_) {
        thread_pool_->AddTask([this, id, term_snapshot, &confirmations, &lost]() {
            raft_msg::HeartBeatRequest req;
            raft_msg::HeartBeatReply resp;
            {
                std::lock_guard<std::mutex> lck(mu_);
                if (state_ != State::kLeader || current_term_ != term_snapshot) {
                    lost.store(true);
                    return;
                }
                req.leader_id = me_;
                req.term = current_term_;
                req.commited_index = commited_index_;
                int prev = next_indexs_[id] - 1;
                if (prev < last_snapshot_index_) {
                    lost.store(true);
                    return;
                }
                req.prev_index = prev;
                req.prev_term = log_[LogPos(prev)].term;
            }
            bool ok = transports_[id]->SendHeartBeat(req, &resp);
            if (!ok) return;
            std::lock_guard<std::mutex> lck(mu_);
            if (state_ != State::kLeader || current_term_ != term_snapshot) {
                lost.store(true);
                return;
            }
            if (resp.term > current_term_) {
                lost.store(true);
                return;
            }
            if (resp.success) {
                confirmations.fetch_add(1);
            }
        });
    }
    int majority = static_cast<int>((peers_.size() + 1) / 2) + 1;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (lost.load()) return -1;
        if (confirmations.load() >= majority) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (confirmations.load() < majority || lost.load()) return -1;
    {
        std::unique_lock<std::mutex> lck(mu_);
        apply_cv_.wait_for(lck, std::chrono::milliseconds(timeout_ms), [&] {
            return last_applied_ >= recorded_commit ||
                   state_ != State::kLeader;
        });
        if (state_ != State::kLeader || last_applied_ < recorded_commit) return -1;
        return recorded_commit;
    }
}

bool RaftNode::LoadSnapshot() {
    std::string data;
    if (!persister_->ReadSnapshot(data)) return false;
    auto nl = data.find('\n');
    if (nl == std::string::npos) return false;
    std::string header = data.substr(0, nl);
    auto colon = header.find(':');
    if (colon == std::string::npos) return false;
    last_snapshot_index_ = std::stoi(header.substr(0, colon));
    last_snapshot_term_ = std::stoi(header.substr(colon + 1));
    std::string sm_data = data.substr(nl + 1);
    state_machine_.ApplySnapshot(sm_data);
    log_.clear();
    log_.push_back(Entry{last_snapshot_index_, last_snapshot_term_, ""});
    if (last_applied_ < last_snapshot_index_) last_applied_ = last_snapshot_index_;
    if (commited_index_ < last_snapshot_index_) commited_index_ = last_snapshot_index_;
    printf("loadSnapshot: index=%d term=%d\n", last_snapshot_index_, last_snapshot_term_);
    return true;
}

void RaftNode::MaybeCreateSnapshot() {
    std::lock_guard<std::mutex> lck(mu_);
    if ((int)log_.size() - 1 < kSnapshotThreshold) return;
    if (last_applied_ <= last_snapshot_index_) return;
    std::string sm_data;
    state_machine_.CreateSnapshot(sm_data);
    std::string snapshot;
    snapshot = std::to_string(last_applied_) + ":" +
               std::to_string(log_[LogPos(last_applied_)].term) + "\n" +
               sm_data;
    if (!persister_->SaveSnapshot(snapshot)) return;
    int snap_idx = last_applied_;
    int snap_term = log_[LogPos(snap_idx)].term;
    int old_pos = LogPos(snap_idx);
    log_.erase(log_.begin(), log_.begin() + old_pos + 1);
    log_.insert(log_.begin(), Entry{snap_idx, snap_term, ""});
    last_snapshot_index_ = snap_idx;
    last_snapshot_term_ = snap_term;
    printf("snapshot created: index=%d term=%d log_size=%d\n",
           snap_idx, snap_term, (int)log_.size());
}

void RaftNode::SendSnapshot(int id, const int kTermSnapshot) {
    raft_msg::InstallSnapshotRequest request;
    raft_msg::InstallSnapshotReply response;
    {
        std::lock_guard<std::mutex> lck(mu_);
        if (!(state_ == State::kLeader && current_term_ == kTermSnapshot)) return;
        request.term = current_term_;
        request.leader_id = me_;
        request.last_snapshot_index = last_snapshot_index_;
        request.last_snapshot_term = last_snapshot_term_;
        std::string full;
        if (!persister_->ReadSnapshot(full)) return;
        auto nl = full.find('\n');
        if (nl != std::string::npos)
            request.snapshot_data = full.substr(nl + 1);
    }
    bool ok = transports_[id]->SendInstallSnapshot(request, &response);
    if (!ok) return;
    std::lock_guard<std::mutex> lck(mu_);
    if (!(state_ == State::kLeader && current_term_ == kTermSnapshot)) return;
    if (response.term > current_term_) {
        current_term_ = response.term;
        vote_for_ = -1;
        state_ = State::kFollower;
        SaveState();
        ResetElectionTimerLocked();
        return;
    }
    if (response.success) {
        next_indexs_[id] = last_snapshot_index_ + 1;
        match_indexs_[id] = last_snapshot_index_;
    }
}

void RaftNode::OnInstallSnapshot(const raft_msg::InstallSnapshotRequest* request,
                                  raft_msg::InstallSnapshotReply* response) {
    std::unique_lock<std::mutex> lck(mu_);
    if (current_term_ < request->term) {
        current_term_ = request->term;
        vote_for_ = -1;
        state_ = State::kFollower;
        SaveState();
    }
    response->term = current_term_;
    if (current_term_ > request->term) {
        response->success = false;
        return;
    }
    ResetElectionTimerLocked();
    if (request->last_snapshot_index <= last_snapshot_index_) {
        response->success = true;
        return;
    }
    std::string full_snapshot;
    full_snapshot = std::to_string(request->last_snapshot_index) + ":" +
                    std::to_string(request->last_snapshot_term) + "\n";
    full_snapshot += request->snapshot_data;
    persister_->SaveSnapshot(full_snapshot);
    state_machine_.ApplySnapshot(request->snapshot_data);
    last_snapshot_index_ = request->last_snapshot_index;
    last_snapshot_term_ = request->last_snapshot_term;
    log_.clear();
    log_.push_back(Entry{last_snapshot_index_, last_snapshot_term_, ""});
    if (last_applied_ < last_snapshot_index_) last_applied_ = last_snapshot_index_;
    if (commited_index_ < last_snapshot_index_) commited_index_ = last_snapshot_index_;
    response->success = true;
    printf("node %d: applied snapshot index=%d term=%d\n",
           me_, last_snapshot_index_, last_snapshot_term_);
}

int RaftNode::LeaseRead(int timeout_ms) {
    {
        std::lock_guard<std::mutex> lck(mu_);
        if (state_ != State::kLeader) return -1;
        if (clock_->Now() < lease_expire_) {
            if (last_applied_ >= commited_index_) return commited_index_;
        }
    }
    return ReadIndex(timeout_ms);
}

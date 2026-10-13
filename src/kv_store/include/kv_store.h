#pragma once
#ifndef KVSTORE_H
#define KVSTORE_H
#include<string>
#include<mutex>
#include<map>
#include<set>
#include "i_state_machine.h"
typedef std::map<std::string,std::string>DataMap;
typedef std::set<std::string>DedupSet;  // client_id:seq
//顶层模块
class KVStore : public IStateMachine
{
private:
    //红黑树存储键值对
    DataMap kv_map_;
    //去重表：记录已执行的 client_id:seq
    DedupSet dedup_set_;
    //资源互斥锁
    std::mutex mu_;
public:
    KVStore();
    ~KVStore();
    //核心接口
    bool Get(const std::string&key,std::string&value);
    bool Put(const std::string&key,const std::string&value) override;
    bool Del(const std::string&key) override;
    //去重检查：返回 true 表示已执行过，false 表示未执行并记录
    bool CheckAndRecord(const std::string& client_id, int seq);
    //创建快照
    void CreateSnapshot(std::string&snapshot) override;
    //从快照恢复状态机
    void ApplySnapshot(const std::string&snapshot) override;
};


#endif //!KVSTORE_H

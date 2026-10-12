#pragma once
#ifndef KVSTORE_H
#define KVSTORE_H
#include<string>
#include<mutex>
#include<map>
#include "i_state_machine.h"
typedef std::map<std::string,std::string>DataMap;
//顶层模块
class KVStore : public IStateMachine
{
private:
    //红黑树存储键值对
    DataMap kv_map_;
    //资源互斥锁
    std::mutex mu_;
public:
    KVStore();
    ~KVStore();
    //核心接口
    bool Get(const std::string&key,std::string&value);
    bool Put(const std::string&key,const std::string&value) override;
    bool Del(const std::string&key) override;
    //创建快照
    void CreateSnapshot(std::string&snapshot) override;
    //从快照恢复状态机
    void ApplySnapshot(const std::string&snapshot) override;
};


#endif //!KVSTORE_H

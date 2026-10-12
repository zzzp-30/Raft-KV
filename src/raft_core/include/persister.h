#pragma once
#ifndef PERSISTER_H
#define PERSISTER_H
#include <mutex>
#include<string>
#include <filesystem>
#include "i_persister.h"
//定义Raft的持久化模块
/*
    保存与恢复
保存状态到raftstate文件
保存日志到raftlog文件
从raftstate中读取状态
从raftlog中读取日志

*/
class Persister : public IPersister
{
private:
    std::mutex mtx_;
    const std::string kPrefixRaftState = "./data/raft-state";
    const std::string kPrefixRaftLog = "./data/raft-log";
    const std::string kPrefixSnapshot = "./data/raft-snapshot";
    std::string file_raft_state_;
    std::string file_raft_log_;
    std::string file_snapshot_;
public:
    //根据id设置文件名
    Persister(int id);
    bool SaveRaftState(const std::string &data) override;
    bool ReadRaftState(std::string&data) override;
    bool SaveRaftLog(const std::string&data) override;
    bool ReadRaftLog(std::string&data) override;
    bool SaveSnapshot(const std::string&data) override;
    bool ReadSnapshot(std::string&data) override;
};


#endif //!PERSISTER_H
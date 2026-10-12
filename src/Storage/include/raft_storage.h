#pragma once

#ifndef RAFTSTORAGE_H
#define RAFTSTORAGE_H
//定义基于raft的分布式存储服务
#include"storage.h"
#include "kv_store.h"
#include "raft_node.h"
class RaftStorage :public Storage
{
    //raft核心引用
    RaftNode& raft_node_;
    //kv状态机引用
    KVStore& state_machine_;

public:

    RaftStorage(RaftNode&raft_node,KVStore &kv_store):Storage(),raft_node_(raft_node),state_machine_(kv_store){}
//实现Storage接口
public:
    bool Put(const std::string&key,const std::string &value)override;
    //获取键对应的值,value=''表示该键不存在
    //return : 成功获取返回true,获取失败返回false
    bool Get(const std::string&key,std::string&value)override;
    //线性一致读: 先确认leader身份,再从状态机读取
    bool ReadIndex(const std::string&key,std::string&value);

};


#endif //!RAFTSTORAGE_H

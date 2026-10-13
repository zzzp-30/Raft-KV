#include"kv_store.h"
#include <mutex>
#include <sstream>
bool KVStore::Get(const std::string&key,std::string&value)
{
    std::lock_guard<std::mutex> lo(mu_);
    if(auto it = kv_map_.find(key);it!=kv_map_.end())
    {
        value =  kv_map_[key];
        return true;
    }
    return false;

}
bool KVStore::Put(const std::string&key,const std::string&value)
{
    std::lock_guard<std::mutex> lo(mu_);
    kv_map_[key] = value;
    printf("PUT ok: %s:%s\n",key.c_str(),value.c_str());
    return true;
}
bool KVStore::Del(const std::string&key)
{
    std::lock_guard<std::mutex> lo(mu_);
    if(auto it = kv_map_.find(key);it!=kv_map_.end())
    {
        kv_map_.erase(key);
        printf("DEL ok: %s\n",key.c_str());
        return true;
    }
    else {
        return false;
    }
}

bool KVStore::CheckAndRecord(const std::string& client_id, int seq) {
    std::lock_guard<std::mutex> lo(mu_);
    std::string key = client_id + ":" + std::to_string(seq);
    if (dedup_set_.count(key) > 0) {
        return true;  // 已执行过
    }
    dedup_set_.insert(key);
    return false;  // 未执行，已记录
}

void KVStore::CreateSnapshot(std::string&snapshot) {
    std::lock_guard<std::mutex> lo(mu_);
    // 序列化 KV 数据
    for(const auto&[key,value] : kv_map_)
    {
        snapshot.append("P:"+key+ ":" + value + "\n");
    }
    // 序列化去重表
    for(const auto& entry : dedup_set_) {
        snapshot.append("D:" + entry + "\n");
    }
}
void KVStore::ApplySnapshot(const std::string&snapshot) {
    std::lock_guard<std::mutex> lo(mu_);
    kv_map_.clear();
    dedup_set_.clear();
    std::istringstream ss(snapshot);
    std::string line;
    while(std::getline(ss, line)) {
        if(line.empty()) continue;
        if (line[0] == 'P') {
            // format: "P:key:value"
            auto first = line.find(':');
            if(first == std::string::npos) continue;
            auto second = line.find(':', first + 1);
            if(second == std::string::npos) continue;
            std::string key = line.substr(first + 1, second - first - 1);
            std::string value = line.substr(second + 1);
            kv_map_[key] = value;
        } else if (line[0] == 'D') {
            // format: "D:client_id:seq"
            auto first = line.find(':');
            if(first == std::string::npos) continue;
            std::string entry = line.substr(first + 1);
            dedup_set_.insert(entry);
        }
    }
}
KVStore::KVStore() {
}
KVStore::~KVStore() {
}
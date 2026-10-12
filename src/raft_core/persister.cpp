
#include "persister.h"
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <sstream>
#include <cstdio>
#ifdef _WIN32
#include <io.h>
#endif
Persister::Persister(int id) {
    std::filesystem::create_directories("./data");
    file_raft_state_ = kPrefixRaftState + std::to_string(id) + ".txt";
    file_raft_log_ = kPrefixRaftLog + std::to_string(id) + ".txt";
    file_snapshot_ = kPrefixSnapshot + std::to_string(id) + ".txt";
}

bool Persister::SaveRaftState(const std::string &data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ofstream ofs;
    ofs.open(file_raft_state_+".tmp",std::ios::out|std::ios::trunc);
    if(!ofs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    ofs << data;
    ofs.close();
#ifdef _WIN32
    std::remove(file_raft_state_.c_str());
#endif
    rename((file_raft_state_+".tmp").c_str(),file_raft_state_.c_str());
    return true;
}

bool Persister::ReadRaftState(std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ifstream ifs;
    ifs.open(file_raft_state_,std::ios::in);
    if(!ifs.is_open()){
        printf("error: file not found!!!\n");
        return false;    
    }
    else if(ifs.peek() == EOF)
    {
        printf("file empty!\n") ;
        return false;
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    data = std::move(buffer.str());
    return true;
}

bool Persister::SaveRaftLog(const std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ofstream ofs;
    ofs.open(file_raft_log_+".tmp",std::ios::out|std::ios::trunc);
    if(!ofs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    ofs << data;
    ofs.close();
#ifdef _WIN32
    std::remove(file_raft_log_.c_str());
#endif
    rename((file_raft_log_+".tmp").c_str(),file_raft_log_.c_str());
    return true;
}

bool Persister::ReadRaftLog(std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ifstream ifs;
    ifs.open(file_raft_log_,std::ios::in);
    if(!ifs.is_open()){
        printf("error: file not found!!!\n");
        return false;    
    }
    else if(ifs.peek() == EOF)
    {
        printf("file empty!\n") ;
        return false;
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    data = std::move(buffer.str());
    return true;
}

bool Persister::SaveSnapshot(const std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ofstream ofs;
    ofs.open(file_snapshot_+".tmp",std::ios::out|std::ios::trunc);
    if(!ofs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    ofs << data;
    ofs.close();
#ifdef _WIN32
    std::remove(file_snapshot_.c_str());
#endif
    rename((file_snapshot_+".tmp").c_str(),file_snapshot_.c_str());
    return true;
}

bool Persister::ReadSnapshot(std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ifstream ifs;
    ifs.open(file_snapshot_,std::ios::in);
    if(!ifs.is_open()){
        return false;    
    }
    else if(ifs.peek() == EOF)
    {
        return false;
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    data = std::move(buffer.str());
    return true;
}

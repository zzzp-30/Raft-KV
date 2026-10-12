#pragma once
#ifndef IPERSISTER_H
#define IPERSISTER_H

#include <string>

// Persistence abstraction: decouples RaftNode from file-based storage.
// In production, Persister implements this with WAL files on disk.
// In testing, MemPersister implements this with in-memory strings.
class IPersister {
public:
    virtual ~IPersister() = default;
    virtual bool SaveRaftState(const std::string& data) = 0;
    virtual bool ReadRaftState(std::string& data) = 0;
    virtual bool SaveRaftLog(const std::string& data) = 0;
    virtual bool ReadRaftLog(std::string& data) = 0;
    virtual bool SaveSnapshot(const std::string& data) = 0;
    virtual bool ReadSnapshot(std::string& data) = 0;
};

#endif  // IPERSISTER_H

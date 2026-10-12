#pragma once
#ifndef MEMPERSISTER_H
#define MEMPERSISTER_H

#include "i_persister.h"
#include <mutex>
#include <string>

// In-memory IPersister implementation for testing.
// Replaces file-based Persister — no disk I/O, instant state reset.
class MemPersister : public IPersister {
private:
    mutable std::mutex mu_;
    std::string state_;
    std::string log_;
    std::string snapshot_;

public:
    bool SaveRaftState(const std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        state_ = data;
        return true;
    }
    bool ReadRaftState(std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (state_.empty()) return false;
        data = state_;
        return true;
    }
    bool SaveRaftLog(const std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        log_ = data;
        return true;
    }
    bool ReadRaftLog(std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        data = log_;
        return true;
    }
    bool SaveSnapshot(const std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        snapshot_ = data;
        return true;
    }
    bool ReadSnapshot(std::string& data) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (snapshot_.empty()) return false;
        data = snapshot_;
        return true;
    }

    // Test-only: simulate crash by discarding all persisted data.
    void Clear() {
        std::lock_guard<std::mutex> lk(mu_);
        state_.clear();
        log_.clear();
        snapshot_.clear();
    }
};

#endif  // MEMPERSISTER_H

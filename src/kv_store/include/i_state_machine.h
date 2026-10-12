#pragma once
#ifndef ISTATEMACHINE_H
#define ISTATEMACHINE_H

#include <string>

// State machine abstraction: decouples RaftNode from KVStore.
// RaftNode only needs Put/Del to apply committed log entries.
// In testing, a mock state machine can record applied commands
// for verification without storing actual key-value data.
class IStateMachine {
public:
    virtual ~IStateMachine() = default;
    virtual bool Put(const std::string& key, const std::string& value) = 0;
    virtual bool Del(const std::string& key) = 0;
    virtual void CreateSnapshot(std::string& snapshot) = 0;
    virtual void ApplySnapshot(const std::string& snapshot) = 0;
};

#endif  // ISTATEMACHINE_H

#pragma once
#ifndef ICLOCK_H
#define ICLOCK_H

#include <chrono>
#include <memory>

class IClock {
public:
    virtual ~IClock() = default;
    virtual std::chrono::steady_clock::time_point Now() const = 0;
};

class SteadyClock : public IClock {
public:
    std::chrono::steady_clock::time_point Now() const override {
        return std::chrono::steady_clock::now();
    }
};

#endif  // ICLOCK_H

#include "fault_injector.h"
#include "mem_transport.h"

void FaultInjector::IsolateNode(int id) {
    hub_->Disconnect(id);
}

void FaultInjector::RestoreNode(int id) {
    hub_->Reconnect(id);
}

void FaultInjector::PartitionPair(int a, int b) {
    hub_->Disconnect(a);
    hub_->Disconnect(b);
}

void FaultInjector::SetUnreliable() {
    hub_->SetReliable(false);
}

void FaultInjector::SetReliable() {
    hub_->SetReliable(true);
}

void FaultInjector::RestoreAll() {
    hub_->SetReliable(true);
    for (int i = 0; i < node_count_; ++i) {
        hub_->Reconnect(i);
    }
}

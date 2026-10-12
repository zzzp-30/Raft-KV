
#include "server.h"
#include <grpcpp/server.h>
#include<thread>
#include "raft_rpc.h"
#include"server_rpc.h"
#include "raft_storage.h"
#include "raft_node.h"
#include "persister.h"
#include "raft_rpc_client.h"
Server::Server(int me,const std::string&address,std::map<int,std::string>peers):me_(me),address_(address),peers_(peers) {


}

bool Server::IsLeader(const ser::isLeaderRequest* request,ser::isLeaderResponse* response) {
    response->set_is_leader(this->raft_node_->IsLeader());
    return true;
}
bool Server::Execute(const ser::executeRequest*request, ser::executeResponse*response) {
    // request->op();
    // auto key = request->key();
    // auto value = request->value();
    // response->set_is_leader(true);
    // response->set_success(true);
    // response->set_key(key);
    // response->set_value(value);
    std::string op = request->op();
    std::string key = request->key();
    std::string value = request->value();
    // std::cout << "[EXECUTE]:" <<op << ":" << key <<" " << value << std::endl;
    bool success;
    if(!this->raft_node_->IsLeader())
    {
        response->set_is_leader(false);
        response->set_success(false);
        return false;
    }
    if(op == "get")
    {
        success = storage_->Get(key,value);
    }else if(op == "put")
    {
        success = storage_->Put(key,value);
    }else if(op == "del")
    {
        success = storage_->Put(key,"");
    }else{
        std::cerr << "[ERROR]:invalid command" ;
        success = false;
    }
    response->set_success(success);
    response->set_is_leader(true);
    if(success)
    {
        response->set_key(key);
        response->set_value(value);
        return true;
    }else{
        return false;
    }
    return true;
}
void Server::Start() {
    //组装子模块
    kv_store_ =  new KVStore;

    std::map<int,Node> nodes;
    for(const auto[id,addr]:peers_)
    {
        nodes[id] = Node(addr);
    }

    //构建传输层:每个peer一个gRPC客户端
    std::map<int,std::shared_ptr<ITransport>> transports;
    for(const auto[id,node]:nodes)
    {
        if(id == me_) continue;
        transports[id] = std::make_shared<RaftRPCClient>(node.address);
    }

    //构建持久化模块
    auto persister = std::make_shared<Persister>(me_);

    raft_node_ = new RaftNode(me_, nodes, std::move(transports), std::move(persister), *kv_store_);

    storage_ = new RaftStorage(*raft_node_,*kv_store_); 
    //启动RPC服务
    StartRPC();

    //启动rpc子模块
    raft_node_->Start();

}
void Server::StartRPC() {
    std::thread th(
        [this](){

            grpc::ServerBuilder builder;
            //绑定地址
            builder.AddListeningPort(
                this->address_,
                grpc::InsecureServerCredentials());
            //注册服务
            //1.与客户端通信的服务
            ServerRPCServiceImpl server_rpc_service(this);
            builder.RegisterService(&server_rpc_service);
            //2.raft节点相互通信的服务
            RaftRPCServiceImpl raft_rpc_service(*(this->raft_node_));
            builder.RegisterService(&raft_rpc_service);
            rpc_server_ = builder.BuildAndStart();
            if (!rpc_server_) {
                std::cerr << "failed to start gRPC server\n";
                return;
            }
            std::cout << "gRPC server listening on " << this->address_ << '\n';
            rpc_server_->Wait();
        }
    );
    th.detach();
}
Server::~Server()
{
    delete raft_node_;
    delete kv_store_;
    delete storage_;
}

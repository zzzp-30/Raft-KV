#include "kv_store.h"
#include "raft_node.h"
#include "raft_rpc.h"
#include "raft_storage.h"
#include "persister.h"
#include "raft_rpc_client.h"
#include <thread>
void Test(Storage & storage,std::string op ,std::string key,std::string value)
{
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::string tip = "[CLI]:" + op + ' ' + key +' ' + value;
    std::cout << tip << std::endl;
    bool res;
    if(op == "put"){
        res = storage.Put(key,value);
    }
    else{
        res = storage.Get(key,value); 
    }
}
//要求输入--me=1@地址:端口 --peers=id@ip:port
int main(int argc,const char*argv[])
{
    printf("program start\n");
    int me;
    std::string address;
    std::map<int,Node> nodes;
    if(argc<2)
    {
        printf("too little arguments\n");
        return -1;
    }else{
        for(int i =1;i<argc;i++)
        {
            std::string buf(argv[i]);
            auto it = buf.find('=');
            auto it2 = buf.find('@');
            int id = stoi(buf.substr(it+1,it2-it-1));
            std::string addr = buf.substr(it2+1);
            if(i == 1)
            {
                me = id;
                address = addr;
            }else{
                nodes.emplace(id,Node(addr));
            }
        }
    }
    KVStore kv_store;
    //构建传输层
    std::map<int,std::shared_ptr<ITransport>> transports;
    for(const auto&[id,node]:nodes)
    {
        transports[id] = std::make_shared<RaftRPCClient>(node.address);
    }
    auto persister = std::make_shared<Persister>(me);
    RaftNode raft_node(me, nodes, std::move(transports), std::move(persister), kv_store);
    RaftServer raft_server;
    raft_server.Start(address,raft_node);
    std::thread th([&](){
        raft_node.Start();
    });
    th.detach();
    RaftStorage storage(raft_node,kv_store);
    while(true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        printf("模拟客户端调用storage接口\n");
        Test(storage,"put","a","123");
        Test(storage,"get","a","");
        Test(storage,"put","a","");
        Test(storage,"get","a","");
    }
    return 0;
}

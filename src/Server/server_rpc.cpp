
#include "server_rpc.h"
#include"server.h"
grpc::Status ServerRPCServiceImpl::isLeader(grpc::ServerContext*,
                          const ser::isLeaderRequest* request,
                          ser::isLeaderResponse* response) {
    if (server_!=nullptr)
    {
        server_->IsLeader(request, response);
    }
    else
    {
        printf("server already destroyed\n");
    }
    return grpc::Status::OK;
}

grpc::Status ServerRPCServiceImpl::execute(grpc::ServerContext*,
                          const ser::executeRequest* request,
                          ser::executeResponse* response) {
    if (server_!=nullptr)
    {
        server_->Execute(request, response);
    }
    else
    {
        printf("server already destroyed\n");
    }
    return grpc::Status::OK;
}


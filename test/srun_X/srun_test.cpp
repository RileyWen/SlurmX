#include"../../src/srun_X/srun_X.h"

int main(int argc, const char* argv[])
{

    GreeterClient greeter(grpc::CreateChannel(
    "localhost:50051", grpc::InsecureChannelCredentials()));
    std::string reply = greeter.submit(greeter.parse(argc, argv));

    fmt::print("Greeter received: {}\n", reply);  
    
    return 0;
}
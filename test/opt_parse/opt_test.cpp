#include"../../src/opt_parse/opt_parse.h"

int main(int argc, const char* argv[])
{
    Command_parse comand_parse;    
    auto re=comand_parse.parse(argc, argv);




    GreeterClient greeter(grpc::CreateChannel(
    "localhost:50051", grpc::InsecureChannelCredentials()));

    std::string reply = greeter.submit(re);

    fmt::print("Greeter received: {}\n", reply);  
    
    return 0;
}
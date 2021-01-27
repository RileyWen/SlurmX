//
// Created by slurm4 on 2021/1/27.
//

#include "../../src/srun_X/srun_async_X.h"

void sig_int(int signo)
{
  printf("sig_int\n");
//  exit(0);
}

int main(int argc, const char* argv[]) {


  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  SubmiterClient submiter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));

  // Spawn reader thread that loops indefinitely
  std::thread thread_ = std::thread(&SubmiterClient::AsyncCompleteRpc, &submiter);


  uint32_t version = 1;
  auto result = submiter.parse(argc, argv);
  submiter.submit(result,version);
  signal(SIGINT, sig_int);

  while(true){
    uint32_t a ;
    std::cin>>a ;
    if(a == 0)
      break;

  }


  std::cout << "Press control-c to quit" << std::endl << std::endl;
  thread_.join();  //blocks forever

  return 0;
}





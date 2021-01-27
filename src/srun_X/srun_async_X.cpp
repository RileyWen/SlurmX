//
// Created by slurm4 on 2021/1/27.
//

#include "srun_async_X.h"
//
// Created by slurm4 on 2021/1/27.
//

SubmiterClient::SubmiterClient(std::shared_ptr<Channel> channel)
    : stub_(Submit::NewStub(channel)) {}

// Assembles the client's payload and sends it to the server.
void SubmiterClient::submit(const cxxopts::ParseResult& result,const std::uint32_t & version) {
  // Data we are sending to the server.
  Command request;

  request.set_version(version);
  std::string str=result["task"].as<std::string>();
  request.set_executive_path(str);

  for(auto arg:result["positional"].as<std::vector<std::string>>()){
    request.add_arguments(arg);
  }

  Command_ResourceLimit* rlimit=request.mutable_rlimit();
  Command_ResourceLimit temp_limit;
  temp_limit.set_cpu_byte(result["ncpu"].as<uint32_t>());

  uint64_t m_temp=0;
  m_temp= memory_parse_client("nmemory",result);
  temp_limit.set_memory_byte(m_temp);

  m_temp= memory_parse_client("nmemory_swap",result);
  temp_limit.set_memory_sw_byte(m_temp);

  m_temp= memory_parse_client("nmemory_soft",result);
  temp_limit.set_memory_ft_byte(m_temp);

  m_temp= memory_parse_client("blockio_weight",result);
  temp_limit.set_blockio_wt_byte(m_temp);

  rlimit->CopyFrom(temp_limit);



  // Call object to store rpc data
  AsyncClientCall* call = new AsyncClientCall;

  // stub_->PrepareAsyncSayHello() creates an RPC object, returning
  // an instance to store in "call" but does not actually start the RPC
  // Because we are using the asynchronous API, we need to hold on to
  // the "call" instance in order to get updates on the ongoing RPC.
  call->response_reader =
      stub_->PrepareAsyncsubmit(&call->context, request, &cq_);

  // StartCall initiates the RPC call
  call->response_reader->StartCall();

  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the memory address of the call object.
  call->response_reader->Finish(&call->reply, &call->status, (void*)call);

}

// Loop while listening for completed responses.
// Prints out the response from the server.
void SubmiterClient::AsyncCompleteRpc() {
  void *got_tag;
  bool ok = false;

  // Block until the next result is available in the completion queue "cq".
  while (cq_.Next(&got_tag, &ok)) {
    // The tag in this example is the memory location of the call object
    AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

    // Verify that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    GPR_ASSERT(ok);

    if (call->status.ok())
      std::cout << "Greeter received: " << call->reply.message() << std::endl;
    else
      std::cout << "RPC failed" << std::endl;

    // Once we're complete, deallocate the call object.
    delete call;
  }

}

//calculate how many bytes
uint64_t SubmiterClient::memory_parse_client(std::string str, const cxxopts::ParseResult &result){
  auto nmemory = result[str].as<std::string>();
  uint64_t nmemory_byte;
  if( nmemory[nmemory.length()-1]=='M' ||  nmemory[nmemory.length()-1] == 'm'){
    nmemory_byte =(uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024;
  }
  else if(nmemory[nmemory.length()-1]=='G' ||  nmemory[nmemory.length()-1]=='g'){
    nmemory_byte = (uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024 * 1024;
  }
  else{
    nmemory_byte=(uint64_t)std::stoi(nmemory.substr(0, nmemory.length()));
  }

  return nmemory_byte;
}


cxxopts::ParseResult SubmiterClient::parse(int argc, const char* argv[])
{

  try
  {
    cxxopts::Options options(argv[0], " - srun command line options");
    options
        .positional_help("task_name [Task Args...]")
        .show_positional_help();
    options
        .add_options()
            ("c,ncpu", "limiting the cpu usage of task", cxxopts::value<uint32_t>()->default_value("2"))
            ("m,nmemory", "limiting the memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
            ("w,nmemory_swap", "limiting the swap memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
            ("s,nmemory_soft", "limiting the soft memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
            ("b,blockio_weight", "limiting the weight of blockio",cxxopts::value<std::string>()->default_value("128M"))
            ("t,task", "task", cxxopts::value<std::string>()->default_value("notask"))
            ("help", "Print help")
            ("positional",
             "Positional arguments: these are the arguments that are entered "
             "without an option", cxxopts::value<std::vector<std::string>>()->default_value(" "));


    options.parse_positional({"task", "positional"});

    auto result = options.parse(argc, argv);

    if (result.count("help"))
    {
      fmt::print("{}",options.help({"", "Group"}));
      exit(0);
    }
    return result;
  }
  catch (const cxxopts::OptionException& e)
  {
    fmt::print("error parsing options: {}\n", e.what());
    exit(1);
  }
}



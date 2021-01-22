#include"opt_parse.h"
//把result写到对应的message
class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Submit::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string submit(const cxxopts::ParseResult& result,std::uint64_t version=1) {
    // Data we are sending to the server.
    Command request;
    /*wzd*/
    request.set_version(version);
    std::string str=result["task"].as<std::string>();
    request.set_executive_path(str);
    //Command_ResourceLimit rlimit_;
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
    //while(true){
//todo 解析到str里面
      //  request.add_arguments(str);    }
    //request.Command_ResourceLimit::set
    






    // Container for the data we expect from the server.
    CommandReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->submit(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {

      return reply.message();
      
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Submit::Stub> stub_;
    //calculate how many bytes
    uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result){
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

};

//把命令行解析成result
class Command_parse{
public:
    //calculate how many bytes
    uint64_t memory_parse(std::string str, const cxxopts::ParseResult &result){
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


    //write memory related option into file
    void Write_StrmArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){

        uint64_t  nmemory_byte =  memory_parse(str,result);
        infile << str <<" limit "<<nmemory_byte<<" bytes "<<std::endl;

    }

    //write string option into file
    void Write_StrArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){

        std::string ss= result[str].as<std::string>();
        infile << str <<" name is "<<  ss <<std::endl;

    }


    //write uint option into file
    void Write_UintArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){

        uint32_t Uint=result[str].as<std::uint32_t>();
        infile << str <<" limit "<<Uint<<" cores "<<std::endl;

    }

    //write vector option into file
    void Write_VetArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){

        infile << str <<" = {";
        auto& v = result[str].as<std::vector<std::string>>();
        for (const auto& s : v) {
            infile << s << ", ";
        }
        infile << "}" << std::endl;

    }

    void Write_Result(std::string str, const cxxopts::ParseResult &result){

        std::ofstream infile;
        infile.open(str,std::ios::out);
        if (!infile.is_open()){
            std::cout << "open file failure" << std::endl;
            return ;
        }
        Write_UintArg("ncpu",result, infile);
        for(std::string str : {"nmemory","nmemory_swap","nmemory_soft","blockio_weight"}){
            Write_StrmArg(str, result,infile);
        }
        Write_StrArg("task",result,infile);
        Write_VetArg("positional",result,infile);
        infile.close();
        std::cout << " The result has been written in "<< str <<std::endl;
    }

    cxxopts::ParseResult parse(int argc, const char* argv[])
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
                std::cout << options.help({"", "Group"}) << std::endl;
                exit(0);
            }


            return result;

        }
        catch (const cxxopts::OptionException& e)
        {
            std::cout << "error parsing options: " << e.what() << std::endl;
            exit(1);
        }
    }
};


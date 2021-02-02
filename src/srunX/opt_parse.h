//
// Created by slurm4 on 2021/2/2.
//

#ifndef SLURMX_OPT_PRASE_H
#define SLURMX_OPT_PRASE_H


#include <cxxopts.hpp>
#include <vector>
#include <regex>

class opt_parse{
 public:

  typedef struct ResourceLimit {
    uint64_t cpu_core_limit;
    uint64_t cpu_shares ;
    uint64_t memory_limit_bytes;
    uint64_t memory_sw_limit_bytes;
    uint64_t memory_soft_limit_bytes;
    uint64_t blockio_weight;
  }ResourceLimit;

  typedef struct TaskInfo{

    std::string executive_path;
    std::vector<std::string> arguments;
    ResourceLimit resourceLimit;

  } TaskInfo;
  cxxopts::ParseResult  parse(int argc, char** argv){
    try
    {
      cxxopts::Options options(argv[0], " - srun command line options");
      options
          .positional_help("task_name [Task Args...]")
          .show_positional_help();
      options
          .add_options()
              ("c,ncpu", "limiting the cpu usage of task", cxxopts::value<uint64_t>()->default_value("2"))
              ("s,ncpu_shares", "limiting the cpu shares of task", cxxopts::value<uint64_t>()->default_value("2"))
              ("m,nmemory", "limiting the memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
              ("w,nmemory_swap", "limiting the swap memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
              ("f,nmemory_soft", "limiting the soft memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
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
//      exit(1);
      throw std::exception();
    }
  }

  uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result){
    auto nmemory = result[str].as<std::string>();
    std::regex Rnmemory("^[0-9]+[mMgG]?$");
    if(!std::regex_match(nmemory,Rnmemory)){
      std::cout<<"error"<<std::endl;
      std::cout<<str<<"must be uint number or the uint number ends with 'm/M/g/G'"<<std::endl;
      throw std::exception();

    }else{
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
  }

  TaskInfo GetTaskInfo(const cxxopts::ParseResult &result){
    TaskInfo task;


    std::regex Rexecutive_path("^\\w*$") ;
    std::string str =  result["task"].as<std::string>();
    if(std::regex_match(str,Rexecutive_path)){
      task.executive_path=str;
    } else{
      std::cout<<"error"<<std::endl;
      std::cout<<"task name can not have '-,|,/,\\,.,*' "<<std::endl;
      throw std::exception();

    }


    for(auto arg : result["positional"].as<std::vector<std::string>>()){
      task.arguments.push_back(arg);
    }

    uint64_t uint ;

    uint = result["ncpu"].as<uint64_t>();
    if(uint==0){
      std::cout<<"error"<<std::endl;
      std::cout<<"cpu core can not be zero "<<std::endl;
      throw std::exception();
    } else{
      task.resourceLimit.cpu_core_limit=uint;
    }


    uint = result["ncpu_shares"].as<uint64_t>();
    if(uint==0){
      std::cout<<"error"<<std::endl;
      std::cout<<"cpu shares can not be zero "<<std::endl;
      throw std::exception();
    } else{
      task.resourceLimit.cpu_shares=uint;
    }


    uint = memory_parse_client("nmemory",result);
    task.resourceLimit.memory_limit_bytes=uint;

    uint = memory_parse_client("nmemory_swap",result);
    task.resourceLimit.memory_sw_limit_bytes=uint;

    uint = memory_parse_client("nmemory_soft",result);
    task.resourceLimit.memory_soft_limit_bytes=uint;


    uint = memory_parse_client("blockio_weight",result);
    task.resourceLimit.blockio_weight=uint;


    return task;


  }



  void PrintTaskInfo(const TaskInfo task){

    fmt::print("executive_path:{}\n",
               task.executive_path
    );

    fmt::print("argments: ");
    for(auto arg : task.arguments){
       fmt::print("{}, ",arg);
    }

    fmt::print("\nResourceLimit:\n cpu_byte:{}\n cpu_shares:{}\n memory_byte:{}\n memory_sw_byte:{}\n memory_ft_byte:{}\n blockio_wt_byte:{}\n",
       task.resourceLimit.cpu_core_limit,
       task.resourceLimit.cpu_shares,
       task.resourceLimit.memory_limit_bytes,
       task.resourceLimit.memory_sw_limit_bytes,
       task.resourceLimit.memory_soft_limit_bytes,
       task.resourceLimit.blockio_weight
    );

  }

};

#endif  // SLURMX_OPT_PRASE_H

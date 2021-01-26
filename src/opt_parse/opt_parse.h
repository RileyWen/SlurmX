#ifndef _opt_parse_
#define _opt_parse_
#include <iostream>
#include <cxxopts.hpp>

#include <fstream>
// #include"grpc_client_test.cpp"
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include <fmt/format.h>

#include "protos/opt.pb.h"
#include "protos/opt.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurm_opt_grpc::Command;
using slurm_opt_grpc::Command_ResourceLimit;
using slurm_opt_grpc::CommandReply;
using slurm_opt_grpc::Submit;
//todo 要重写 明天吧
class GreeterClient
{
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel);
  std::string submit(const cxxopts::ParseResult& result,std::uint64_t version=1);
 private:
  std::unique_ptr<Submit::Stub> stub_;
  uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result);
};

class Command_parse{
 public:
  uint64_t memory_parse(std::string str, const cxxopts::ParseResult &result);
  void Write_StrmArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile);
  void Write_UintArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile);
  void Write_VetArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile);
  void Write_Result(std::string str, const cxxopts::ParseResult &result);
  cxxopts::ParseResult parse(int argc, const char* argv[]);
  void Write_StrArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile);



};
#endif


//class GreeterClient {
// public:
//
//
//  //
//  std::unique_ptr<Submit::Stub> stub_;
//};

//把命令行解析成result
//class Command_parse{
// public:
//  //calculate how many bytes
//  uint64_t memory_parse(std::string str, const cxxopts::ParseResult &result){
//    auto nmemory = result[str].as<std::string>();
//    uint64_t nmemory_byte;
//    if( nmemory[nmemory.length()-1]=='M' ||  nmemory[nmemory.length()-1] == 'm'){
//      nmemory_byte =(uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024;
//    }
//    else if(nmemory[nmemory.length()-1]=='G' ||  nmemory[nmemory.length()-1]=='g'){
//      nmemory_byte = (uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024 * 1024;
//    }
//    else{
//      nmemory_byte=(uint64_t)std::stoi(nmemory.substr(0, nmemory.length()));
//    }
//
//    return nmemory_byte;
//  }
//
//
//
//  //write string option into file
//  void Write_StrArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){
//
//    std::string ss= result[str].as<std::string>();
//    infile << str <<" name is "<<  ss <<std::endl;
//
//  }
//
//
//  //write uint option into file
//  void Write_UintArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){
//
//    uint32_t Uint=result[str].as<std::uint32_t>();
//    infile << str <<" limit "<<Uint<<" cores "<<std::endl;
//
//  }
//
//  //write vector option into file
//  void Write_VetArg(std::string str, const cxxopts::ParseResult &result, std::ofstream &infile){
//
//    infile << str <<" = {";
//    auto& v = result[str].as<std::vector<std::string>>();
//    for (const auto& s : v) {
//      infile << s << ", ";
//    }
//    infile << "}" << std::endl;
//
//  }
//
//  void Write_Result(std::string str, const cxxopts::ParseResult &result){
//
//    std::ofstream infile;
//    infile.open(str,std::ios::out);
//    if (!infile.is_open()){
//      std::cout << "open file failure" << std::endl;
//      return ;
//    }
//    Write_UintArg("ncpu",result, infile);
//    for(std::string str : {"nmemory","nmemory_swap","nmemory_soft","blockio_weight"}){
//      Write_StrmArg(str, result,infile);
//    }
//    Write_StrArg("task",result,infile);
//    Write_VetArg("positional",result,infile);
//    infile.close();
//    std::cout << " The result has been written in "<< str <<std::endl;
//  }
//
//  cxxopts::ParseResult parse(int argc, const char* argv[])
//  {
//
//    try
//    {
//      cxxopts::Options options(argv[0], " - srun command line options");
//      options
//          .positional_help("task_name [Task Args...]")
//          .show_positional_help();
//      options
//          .add_options()
//              ("c,ncpu", "limiting the cpu usage of task", cxxopts::value<uint32_t>()->default_value("2"))
//              ("m,nmemory", "limiting the memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
//              ("w,nmemory_swap", "limiting the swap memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
//              ("s,nmemory_soft", "limiting the soft memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
//              ("b,blockio_weight", "limiting the weight of blockio",cxxopts::value<std::string>()->default_value("128M"))
//              ("t,task", "task", cxxopts::value<std::string>()->default_value("notask"))
//              ("help", "Print help")
//              ("positional",
//               "Positional arguments: these are the arguments that are entered "
//               "without an option", cxxopts::value<std::vector<std::string>>()->default_value(" "));
//
//
//      options.parse_positional({"task", "positional"});
//
//      auto result = options.parse(argc, argv);
//
//      if (result.count("help"))
//      {
//        std::cout << options.help({"", "Group"}) << std::endl;
//        exit(0);
//      }
//
//
//      return result;
//
//    }
//    catch (const cxxopts::OptionException& e)
//    {
//      std::cout << "error parsing options: " << e.what() << std::endl;
//      exit(1);
//    }
//  }
//};
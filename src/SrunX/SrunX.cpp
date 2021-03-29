#include "CommandLineParse.h"
#include "PublicHeader.h"
#include "SrunXClient.h"

int main(int argc, char** argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  SlurmxErr err;
  SrunX::CommandLineArgs args;

  try {
    cxxopts::Options options = SrunX::InitOptions();
    cxxopts::ParseResult parse_result = options.parse(argc, argv);

    err = SrunX::CheckValidityOfCommandLineArgs(options, parse_result);
    if (err != SlurmxErr::kOk) return 1;

    err = SrunX::ParseCommandLineArgsIntoStruct(parse_result, &args);
    if (err != SlurmxErr::kOk) return 1;

  } catch (const cxxopts::OptionException& e) {
    fmt::print("Invalid arguments: {}\n", e.what());
    return 1;
  }

  std::string xd_addr_port =
      fmt::format("{}:{}", args.xd_address, args.xd_port);
  std::string ctlxd_addr_port =
      fmt::format("{}:{}", args.ctlxd_address, args.ctlxd_port);

  SrunX::SrunXClient client;

  err = client.Init(xd_addr_port, ctlxd_addr_port);
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  }

  err = client.Run(args);
  client.Wait();

  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  } else
    return 0;
}
#pragma once

#include <cxxopts.hpp>

#include "PublicHeader.h"

namespace SrunX {

struct CommandLineArgs {
  resource_t required_resource;

  std::string ctlxd_address;
  std::string ctlxd_port;
  std::string xd_address;
  std::string xd_port;

  std::string executive_path;
  std::vector<std::string> arguments;
};

cxxopts::Options InitOptions();

SlurmxErr CheckValidityOfCommandLineArgs(
    const cxxopts::Options options, const cxxopts::ParseResult& parse_result);

/**
 * use cxxopts::Options to parse command line arguments into a CommandLineArgs
 * struct.
 * @param[in] parse_result a cxxopts::Options instance
 * @param[out] args the parsed CommandLineArgs struct
 * @return kOk if successful, kGenericFailure otherwise
 */
SlurmxErr ParseCommandLineArgsIntoStruct(
    const cxxopts::ParseResult& parse_result, CommandLineArgs* args);

}  // namespace SrunX

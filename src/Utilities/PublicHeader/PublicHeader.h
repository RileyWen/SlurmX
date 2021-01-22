#pragma once

#include "protos/slurmx.pb.h"
#include "spdlog/spdlog.h"

#define SLURMX_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define SLURMX_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLURMX_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define SLURMX_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define SLURMX_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define SLURMX_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

enum class SlurmxErr : uint16_t {
  kOk = 0,
  kGenericFailure,
  kNoResource,
  kNonExistent,
  kSystemErr,
  kExistingTask,
  kInvalidParam,
  kStop,
  kConnectionTimeout,
  kConnectionAborted,

  kRpcFailed,
  kNoTokenReply,
  kNewTaskFailed,
  KStreamBroken,
  __ERR_SIZE
};

inline const char* kCtlXdDefaultPort = "10011";
inline const char* kXdDefaultPort = "10010";

namespace Internal {

constexpr std::array<std::string_view, uint16_t(SlurmxErr::__ERR_SIZE)>
    SlurmxErrStrArr = {
        "Success",
        "Generic failure",
        "Resource not enough",
        "The object doesn't exist",
        "Linux Error",
        "Task already exists",
        "Invalid Parameter",
        "The owner object of the function is stopping",
        "Connection timeout",
        "Connection aborted",

        "PRC Failed",
        "Can not get token from SlurmCtlXd",
        "New Task create failed",
        "Stream is broken",

};

}

inline std::string_view SlurmxErrStr(SlurmxErr err) {
  return Internal::SlurmxErrStrArr[uint16_t(err)];
}

// Model the allocatable resources on a slurmxd node.
struct resource_t {
  uint32_t cpu_count = 0;
  uint64_t memory_bytes = 0;
  uint64_t memory_sw_bytes = 0;

  resource_t() = default;
  resource_t(const slurmx_grpc::AllocatableResource&);

  resource_t& operator+=(const resource_t& rhs);

  resource_t& operator-=(const resource_t& rhs);
};

bool operator<=(const resource_t& lhs, const resource_t& rhs);
bool operator<(const resource_t& lhs, const resource_t& rhs);

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
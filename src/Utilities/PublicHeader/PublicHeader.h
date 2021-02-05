#pragma once

#include "spdlog/spdlog.h"

#define SLURMX_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define SLURMX_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLURMX_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define SLURMX_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define SLURMX_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define SLURMX_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

enum class SlurmxErr : uint16_t {
  OK = 0,
  GENERIC_FAILURE,
  NO_RESOURCE,

  __ERR_SIZE
};

namespace Internal {

constexpr std::array<std::string_view, uint16_t(SlurmxErr::__ERR_SIZE)>
    SlurmxErrStrArr = {
        "Success",
        "Generic failure",
        "Resource not enough",
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

  resource_t& operator+=(const resource_t& rhs);

  resource_t& operator-=(const resource_t& rhs);
};

bool operator<=(const resource_t& lhs, const resource_t& rhs);
bool operator<(const resource_t& lhs, const resource_t& rhs);

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%C-%m-%d %s:%#] %v"); }
};

[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
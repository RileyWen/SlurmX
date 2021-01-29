#pragma once

#include "spdlog/spdlog.h"

#define SLURMX_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define SLURMX_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLURMX_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define SLURMX_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define SLURMX_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define SLURMX_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%C-%m-%d %s:%#] %v"); }
};

[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
#pragma once

#include "protos/slurmx.pb.h"
#include "spdlog/spdlog.h"

#define SLURMX_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define SLURMX_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLURMX_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define SLURMX_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define SLURMX_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define SLURMX_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

#ifndef NDEBUG
#define SLURMX_ASSERT(condition, ...) \
  do {                                \
    if (!(condition)) {               \
      SLURMX_CRITICAL(__VA_ARGS__);   \
      std::terminate();               \
    }                                 \
  } while (false)
#else
#define ASSERT(condition, message) \
  do {                             \
  } while (false)
#endif

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

  kRpcFailure,
  kTokenRequestFailure,
  KStreamBroken,
  kInvalidStub,

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

        "RPC call failed",
        "Failed to request required token",
        "Stream is broken",
        "Xd node stub is invalid",
};

}

inline std::string_view SlurmxErrStr(SlurmxErr err) {
  return Internal::SlurmxErrStrArr[uint16_t(err)];
}

/* ----------- Public definitions for all components */

// (partition id, node index), by which a Xd node is uniquely identified.
struct XdNodeId {
  uint32_t partition_id;
  uint32_t node_index;

  struct Hash {
    std::size_t operator()(const XdNodeId& val) const {
      return std::hash<uint64_t>()(
          (static_cast<uint64_t>(val.partition_id) << 32) |
          static_cast<uint64_t>(val.node_index));
    }
  };
};

inline bool operator==(const XdNodeId& lhs, const XdNodeId& rhs) {
  return (lhs.node_index == rhs.node_index) &&
         (lhs.partition_id == rhs.partition_id);
}

/**
 * Custom formatter for XdNodeId in fmt.
 */
template <>
struct fmt::formatter<XdNodeId> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const XdNodeId& id, FormatContext& ctx) -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return format_to(ctx.out(), "({}, {})", id.partition_id, id.node_index);
  }
};

// Model the allocatable resources on a slurmxd node.
struct resource_t {
  uint32_t cpu_count = 0;
  uint64_t memory_bytes = 0;
  uint64_t memory_sw_bytes = 0;

  resource_t() = default;
  resource_t(const SlurmxGrpc::AllocatableResource&);

  resource_t& operator+=(const resource_t& rhs);

  resource_t& operator-=(const resource_t& rhs);
};

bool operator<=(const resource_t& lhs, const resource_t& rhs);
bool operator<(const resource_t& lhs, const resource_t& rhs);

struct job_info_t {

  uint32_t job_id;  //job id
  std::string job_name;
  uint32_t user_id;	// user the job runs as
  std::string user_name;
  std::string state_desc;

  resource_t alloc_res;
};

struct JobInfoMsg{
  std::string last_update;	//time of latest info
  std::vector<job_info_t> job_array;
};

//Used as show_flags for slurm_get_ and slurm_load_ function calls.
#define SHOW_ALL	0x0001	/* Show info for "hidden" partitions */
#define SHOW_DETAIL	0x0002	/* Show detailed resource information */
#define SHOW_DETAIL2	0x0004	/* Show batch script listing */
#define SHOW_MIXED	0x0008	/* Automatically set node MIXED state */
#define SHOW_LOCAL	0x0010	/* Show only local information, even on
				 * federated cluster */
#define SHOW_SIBLING	0x0020	/* Show sibling jobs on a federated cluster */
#define SHOW_FEDERATION	0x0040	/* Show federated state information.
				 * Shows local info if not in federation */
#define SHOW_FUTURE	0x0080	/* Show future nodes */

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
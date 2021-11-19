#pragma once

#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)
#include <spdlog/spdlog.h>

#include <boost/uuid/uuid.hpp>
#include <list>

#include "protos/slurmx.pb.h"

// For better logging inside lambda functions
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#define __FUNCTION__ __PRETTY_FUNCTION__
#endif

#define SLURMX_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define SLURMX_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLURMX_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define SLURMX_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define SLURMX_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define SLURMX_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

#ifndef NDEBUG
#define SLURMX_ASSERT(condition, ...)                                        \
  do {                                                                       \
    if (!(condition)) {                                                      \
      SLURMX_CRITICAL("Assertion failed: \"" #condition "\".", __VA_ARGS__); \
      std::terminate();                                                      \
    }                                                                        \
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
  kSystemErr,  // represent the error which sets errno
  kExistingTask,
  kInvalidParam,
  kStop,
  kConnectionTimeout,
  kConnectionAborted,

  kRpcFailure,
  kTokenRequestFailure,
  KStreamBroken,
  kInvalidStub,
  kCgroupError,
  kProtobufError,
  kLibEventError,

  __ERR_SIZE  // NOLINT(bugprone-reserved-identifier)
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
        "Error when manipulating cgroup",
        "Error when using protobuf",
        "Error when using LibEvent",
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
// It contains CPU and memory by now.
struct AllocatableResource {
  uint32_t cpu_count = 0;
  uint64_t memory_bytes = 0;
  uint64_t memory_sw_bytes = 0;

  AllocatableResource() = default;
  explicit AllocatableResource(const SlurmxGrpc::AllocatableResource&);
  AllocatableResource& operator=(const SlurmxGrpc::AllocatableResource&);

  AllocatableResource& operator+=(const AllocatableResource& rhs);

  AllocatableResource& operator-=(const AllocatableResource& rhs);
};

bool operator<=(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator==(const AllocatableResource& lhs, const AllocatableResource& rhs);

/**
 * Model the dedicated resources in a slurmxd node.
 * It contains GPU, NIC, etc.
 */
struct DedicatedResource {};

/**
 * When a task is allocated a resource UUID, it holds one instance of Resources
 * struct. Resource struct contains a AllocatableResource struct and a list of
 * DedicatedResource.
 */
struct Resources {
  AllocatableResource allocatable_resource;

  Resources() = default;

  Resources& operator+=(const Resources& rhs);
  Resources& operator-=(const Resources& rhs);

  Resources& operator+=(const AllocatableResource& rhs);
  Resources& operator-=(const AllocatableResource& rhs);
};

bool operator<=(const Resources& lhs, const Resources& rhs);
bool operator<(const Resources& lhs, const Resources& rhs);
bool operator==(const Resources& lhs, const Resources& rhs);

struct ITask {
  enum class Type { Interactive, Batch };
  enum class Status { Pending, Running, Completing, Finished, Failed };

  /* -------- Fields that are set at the submission time. ------- */
  absl::Duration time_limit;

  std::string partition_name;
  Resources resources;

  Type type;

  /* ------- Fields that won't change after this task is accepted. -------- */
  uint32_t task_id;
  uint32_t partition_id;

  /* ----- Fields that may change at run time. ----------- */
  Status status;
  uint32_t node_index;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time start_time;

  absl::Time end_time;

  virtual ~ITask() = default;

 protected:
  ITask() = default;
};

struct InteractiveTask : public ITask {
  using ITask::ITask;
  boost::uuids::uuid resource_uuid;
};

struct BatchTask : public ITask {
  using ITask::ITask;
  std::string executive_path;
  std::list<std::string> arguments;
  std::string output_file_pattern;
};

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
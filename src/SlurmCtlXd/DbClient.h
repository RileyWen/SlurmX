#pragma once

#include <mysql.h>
#include <spdlog/fmt/fmt.h>

#include <list>
#include <memory>
#include <string>

#include "CtlXdPublicDefs.h"
#include "slurmx/PublicHeader.h"

namespace CtlXd {

class MariadbClient {
 public:
  MariadbClient() noexcept = default;

  ~MariadbClient();

  bool Init();

  bool Connect(const std::string& username, const std::string& password);

  bool GetMaxExistingJobId(uint64_t* job_id);

  bool GetLastInsertId(uint64_t* id);

  bool InsertJob(uint64_t* job_db_inx, uint64_t mod_timestamp,
                 const std::string& account, uint32_t cpu,
                 uint64_t memory_bytes, const std::string& job_name,
                 const std::string& env, uint32_t id_job, uid_t id_user,
                 uid_t id_group, const std::string& nodelist,
                 uint32_t nodes_alloc, const std::string& node_inx,
                 const std::string& partition_name, uint32_t priority,
                 uint64_t submit_timestamp, const std::string& script,
                 uint32_t state, uint32_t timelimit,
                 const std::string& work_dir,
                 const SlurmxGrpc::TaskToCtlXd& task_to_ctlxd);

  bool FetchJobRecordsWithStates(
      std::list<CtlXd::TaskInCtlXd>* task_list,
      const std::list<SlurmxGrpc::TaskStatus>& states);

  bool UpdateJobRecordField(uint64_t job_db_inx, const std::string& field_name,
                            const std::string& val);

  bool UpdateJobRecordFields(uint64_t job_db_inx,
                             const std::list<std::string>& field_name,
                             const std::list<std::string>& val);

 private:
  void PrintError_(const std::string& msg) {
    SLURMX_ERROR("{}: {}\n", msg, mysql_error(conn));
  }

  void PrintError_(const char* msg) {
    SLURMX_ERROR("{}: {}\n", msg, mysql_error(conn));
  }

  MYSQL* conn{nullptr};
  const std::string m_db_name{"slurmx_db"};
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::MariadbClient> g_db_client;
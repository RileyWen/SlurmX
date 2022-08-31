#pragma once

#include <mysql.h>
#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <list>
#include <memory>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>
#include <string>

#include "CtlXdPublicDefs.h"
#include "slurmx/PublicHeader.h"

using namespace mongocxx;
using bsoncxx::builder::stream::document;

namespace CtlXd {

class MariadbClient {
 public:
  MariadbClient() = default;

  ~MariadbClient();

  bool Init();

  void SetUserAndPwd(const std::string& username, const std::string& password);

  bool Connect();

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
    SLURMX_ERROR("{}: {}\n", msg, mysql_error(m_conn));
  }

  void PrintError_(const char* msg) {
    SLURMX_ERROR("{}: {}\n", msg, mysql_error(m_conn));
  }

  MYSQL* m_conn{nullptr};
  const std::string m_db_name{"slurmx_db"};

  std::string m_user;
  std::string m_psw;
};

class MongodbClient {
 public:
  enum EntityType {
    Account = 0,
    User = 1,
    Qos = 2,
  };
  struct MongodbResult {
    bool ok{false};
    std::optional<std::string> reason;
  };
  struct MongodbFilter {
    enum RelationalOperator {
      Equal,
      Greater,
      Less,
      GreaterOrEqual,
      LessOrEqual
    };
    std::string object;
    RelationalOperator relateOperator;
    std::string value;
  };
  MongodbClient() = default;

  ~MongodbClient();

  bool Connect();

  void Init();

  MongodbResult AddUser(const CtlXd::User& new_user);
  MongodbResult AddAccount(const CtlXd::Account& new_account);
  MongodbResult AddQos(const CtlXd::Qos& new_qos);

  MongodbResult DeleteEntity(EntityType type, const std::string& name);

  bool GetUserInfo(const std::string& name, CtlXd::User* user);
  bool GetExistedUserInfo(const std::string& name, CtlXd::User* user);
  bool GetAllUserInfo(std::list<CtlXd::User>& user_list);
  bool GetAccountInfo(const std::string& name, CtlXd::Account* account);
  bool GetExistedAccountInfo(const std::string& name, CtlXd::Account* account);
  bool GetAllAccountInfo(std::list<CtlXd::Account>& account_list);
  bool GetQosInfo(const std::string& name, CtlXd::Qos* qos);

  MongodbResult SetUser(const CtlXd::User& new_user);
  MongodbResult SetAccount(const CtlXd::Account& new_account);

  std::list<std::string> GetUserAllowedPartition(const std::string& name);
  std::list<std::string> GetAccountAllowedPartition(const std::string& name);

  bool SetUserAllowedPartition(const std::string& name,
                               const std::list<std::string>& partitions,
                               SlurmxGrpc::ModifyEntityRequest::Type type);
  bool SetAccountAllowedPartition(const std::string& name,
                                  const std::list<std::string>& partitions,
                                  SlurmxGrpc::ModifyEntityRequest::Type type);

 private:
  const std::string m_db_name{"slurmx_db"};
  const std::string m_account_collection_name{"acct_table"};
  const std::string m_user_collection_name{"user_table"};
  const std::string m_qos_collection_name{"qos_table"};

  mongocxx::instance* m_dbInstance{nullptr};
  mongocxx::client* m_client = {nullptr};
  mongocxx::database* m_database = {nullptr};
  mongocxx::collection *m_account_collection{nullptr},
      *m_user_collection{nullptr}, *m_qos_collection{nullptr};
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::MariadbClient> g_db_client;
inline std::unique_ptr<CtlXd::MongodbClient> g_mongodb_client;
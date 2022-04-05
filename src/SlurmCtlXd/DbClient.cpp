#include "DbClient.h"

#include <absl/strings/str_join.h>

namespace CtlXd {

MariadbClient::~MariadbClient() {
  if (conn) {
    mysql_close(conn);
  }
}

bool MariadbClient::Init() {
  conn = mysql_init(nullptr);
  return conn != nullptr;
}

bool MariadbClient::Connect(const std::string& username,
                            const std::string& password) {
  if (mysql_real_connect(conn, "127.0.0.1", username.c_str(), password.c_str(),
                         nullptr, 3306, nullptr, 0) == nullptr) {
    PrintError_("Cannot connect to database");
    return false;
  }

  if (mysql_query(conn, "CREATE DATABASE IF NOT EXISTS slurmx_db;")) {
    PrintError_("Cannot check the existence of slurmx_db");
    return false;
  }

  if (mysql_select_db(conn, m_db_name.c_str()) != 0) {
    PrintError_("Cannot select slurmx_db");
    return false;
  }

  if (mysql_query(
          conn,
          "CREATE TABLE IF NOT EXISTS job_table("
          "job_db_inx    bigint unsigned not null auto_increment primary key,"
          "mod_time        bigint unsigned default 0 not null,"
          "deleted         tinyint         default 0 not null,"
          "account         tinytext,"
          "cpus_req        int unsigned              not null,"
          "mem_req         bigint unsigned default 0 not null,"
          "job_name        tinytext                  not null,"
          "env             text,"
          "id_job          int unsigned              not null,"
          "id_user         int unsigned              not null,"
          "id_group        int unsigned              not null,"
          "nodelist        text,"
          "nodes_alloc     int unsigned              not null,"
          "node_inx        text,"
          "partition_name  tinytext                  not null,"
          "priority        int unsigned              not null,"
          "time_eligible   bigint unsigned default 0 not null,"
          "time_start      bigint unsigned default 0 not null,"
          "time_end        bigint unsigned default 0 not null,"
          "time_suspended  bigint unsigned default 0 not null,"
          "script          text                      not null default '',"
          "state           int unsigned              not null,"
          "timelimit       int unsigned    default 0 not null,"
          "time_submit     bigint unsigned default 0 not null,"
          "work_dir        text                      not null default '',"
          "submit_line     text"
          ");")) {
    PrintError_("Cannot check the existence of job_table");
    return false;
  }

  return true;
}

bool MariadbClient::GetMaxExistingJobId(uint64_t* job_id) {
  if (mysql_query(conn,
                  "SELECT COALESCE(MAX(job_db_inx), 0) FROM job_table;")) {
    PrintError_("Cannot get the max id");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(conn);
  if (result == nullptr) {
    PrintError_("Error in getting the max job id result");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  unsigned long* lengths = mysql_fetch_lengths(result);

  if (lengths == nullptr) {
    PrintError_("Error in fetching rows of max id result");
    mysql_free_result(result);
    return false;
  }

  *job_id = strtoul(row[0], nullptr, 10);

  mysql_free_result(result);
  return true;
}

bool MariadbClient::GetLastInsertId(uint64_t* id) {
  if (mysql_query(conn, "SELECT LAST_INSERT_ID();")) {
    PrintError_("Cannot get last insert id");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(conn);
  if (result == nullptr) {
    PrintError_("Error in getting the max job id result");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  unsigned long* lengths = mysql_fetch_lengths(result);

  if (lengths == nullptr) {
    PrintError_("Error in fetching rows of max id result");
    mysql_free_result(result);
    return false;
  }

  *id = strtoul(row[0], nullptr, 10);

  mysql_free_result(result);
  return true;
}

bool MariadbClient::UpdateJobRecordField(uint64_t job_db_inx,
                                         const std::string& field_name,
                                         const std::string& val) {
  std::string query = fmt::format(
      "UPDATE job_table SET {} = '{}', mod_time = UNIX_TIMESTAMP() WHERE "
      "job_db_inx = {};",
      field_name, val, job_db_inx);

  if (mysql_query(conn, query.c_str())) {
    PrintError_("Failed to update job record");
    return false;
  }

  return true;
}

bool MariadbClient::UpdateJobRecordFields(
    uint64_t job_db_inx, const std::list<std::string>& field_names,
    const std::list<std::string>& values) {
  SLURMX_ASSERT(field_names.size() == values.size() && !field_names.empty());

  std::vector<std::string> kvs;
  for (auto it_k = field_names.begin(), it_v = values.begin();
       it_k != field_names.end() && it_v != values.end(); ++it_k, ++it_v) {
    std::string piece = fmt::format("{} = '{}'", *it_k, *it_v);
    kvs.emplace_back(std::move(piece));
  }

  std::string formatter = absl::StrJoin(kvs, ", ");

  std::string query = fmt::format(
      "UPDATE job_table SET {}, mod_time = UNIX_TIMESTAMP() "
      "WHERE job_db_inx = {};",
      formatter, job_db_inx);

  if (mysql_query(conn, query.c_str())) {
    PrintError_("Failed to update job record");
    return false;
  }

  return true;
}

bool MariadbClient::FetchJobRecordsWithStates(
    std::list<TaskInCtlXd>* task_list,
    const std::list<SlurmxGrpc::TaskStatus>& states) {
  std::vector<std::string> state_piece;
  for (auto state : states) {
    state_piece.emplace_back(fmt::format("state = {}", state));
  }
  std::string state_str = absl::StrJoin(state_piece, " or ");

  std::string query =
      fmt::format("SELECT * FROM job_table WHERE {};", state_str);

  if (mysql_query(conn, query.c_str())) {
    PrintError_("Failed to fetch job record");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(conn);
  if (result == nullptr) {
    PrintError_("Error in getting `fetch job` result");
    return false;
  }

  uint32_t num_fields = mysql_num_fields(result);

  MYSQL_ROW row;
  // 0  job_db_inx     mod_time       deleted       account     cpus_req
  // 5  mem_req        job_name       env           id_job      id_user
  // 10 id_group       nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority       time_submit    time_eligible time_start  time_end
  // 20 time_suspended script         state         timelimit   work_dir
  // 25 submit_line    task_to_ctlxd

  while ((row = mysql_fetch_row(result))) {
    size_t* lengths = mysql_fetch_lengths(result);

    CtlXd::TaskInCtlXd task;
    task.job_db_inx = std::strtoul(row[0], nullptr, 10);
    task.resources.allocatable_resource.cpu_count =
        std::strtoul(row[4], nullptr, 10);
    task.resources.allocatable_resource.memory_bytes =
        task.resources.allocatable_resource.memory_sw_bytes =
            std::strtoul(row[5], nullptr, 10);
    task.name = row[6];
    task.env = row[7];
    task.task_id = std::strtoul(row[8], nullptr, 10);
    task.uid = std::strtoul(row[9], nullptr, 10);
    task.gid = std::strtoul(row[10], nullptr, 10);
    task.partition_name = row[14];

    task.meta = CtlXd::BatchMetaInTask{};
    auto& batch_meta = std::get<CtlXd::BatchMetaInTask>(task.meta);
    batch_meta.sh_script = row[21];
    task.status = SlurmxGrpc::Pending;
    task.time_limit = absl::Seconds(std::strtol(row[23], nullptr, 10));
    task.cwd = row[24];

    if (row[25]) task.cmd_line = row[25];

    bool ok = task.task_to_ctlxd.ParseFromArray(row[26], lengths[26]);

    task_list->emplace_back(std::move(task));
  }

  mysql_free_result(result);
  return true;
}

bool MariadbClient::InsertJob(
    uint64_t* job_db_inx, uint64_t mod_timestamp, const std::string& account,
    uint32_t cpu, uint64_t memory_bytes, const std::string& job_name,
    const std::string& env, uint32_t id_job, uid_t id_user, uid_t id_group,
    const std::string& nodelist, uint32_t nodes_alloc,
    const std::string& node_inx, const std::string& partition_name,
    uint32_t priority, uint64_t submit_timestamp, const std::string& script,
    uint32_t state, uint32_t timelimit, const std::string& work_dir,
    const SlurmxGrpc::TaskToCtlXd& task_to_ctlxd) {
  size_t blob_size = task_to_ctlxd.ByteSizeLong();
  char blob[1024];
  char query[2048];
  task_to_ctlxd.SerializeToArray(blob, 1024);

  std::string query_head = fmt::format(
      "INSERT INTO job_table("
      "mod_time, deleted, account, cpus_req, mem_req, job_name, env, "
      "id_job, id_user, id_group, nodelist, nodes_alloc, node_inx, "
      "partition_name, priority, time_submit, script, state, timelimit, "
      " work_dir, task_to_ctlxd) "
      " VALUES({}, 0, '{}', {}, {}, '{}', '{}', {}, {}, {}, "
      "'{}', {}, '{}', '{}', {}, {}, '{}', {}, {}, "
      "'{}', '",
      mod_timestamp, account, cpu, memory_bytes, job_name, env, id_job, id_user,
      id_group, nodelist, nodes_alloc, node_inx, partition_name, priority,
      submit_timestamp, script, state, timelimit, work_dir);
  char* query_ptr = std::copy(query_head.c_str(),
                              query_head.c_str() + query_head.size(), query);
  size_t escaped_size =
      mysql_real_escape_string(conn, query_ptr, blob, blob_size);
  query_ptr += escaped_size;

  const char query_end[] = "')";
  query_ptr =
      std::copy(query_end, query_end + sizeof(query_end) - 1, query_ptr);

  if (mysql_real_query(conn, query, query_ptr - query)) {
    PrintError_("Failed to insert job record");
    return false;
  }

  uint64_t last_id;
  if (!GetLastInsertId(&last_id)) {
    PrintError_("Failed to GetLastInsertId");
    return false;
  }
  *job_db_inx = last_id;

  return true;
}

}  // namespace CtlXd

#include "SqueueXClient.h"

namespace SqueueX {

SlurmxErr SqueueXClient::GetInfo(bool clear_old, SqueueParameters &params) {
  static JobInfoMsg old_job;
  JobInfoMsg new_job_info;
  SlurmxErr error_code;
  uint16_t show_flags = SHOW_ALL;

  if (params.job_id) {
    error_code = LoadJobByJobId(&new_job_info, params.job_id, show_flags);
  } else if (params.user_id) {
    error_code = LoadJobsByUserId(&new_job_info, params.user_id, show_flags);
  } else {
    error_code = LoadJobs(absl::Now(), &new_job_info, show_flags);
  }

  if (error_code != SlurmxErr::kOk) {
    SPDLOG_ERROR("Load jobs error.");
    return SlurmxErr::kGenericFailure;
  }

  PrintJobArray(new_job_info);

  return SlurmxErr::kOk;
}

SlurmxErr SqueueXClient::Init(std::string ctlxd_addr_port) {
  channel_ =
      grpc::CreateChannel(ctlxd_addr_port, grpc::InsecureChannelCredentials());
  bool ok;

  stub_ = SlurmCtlXd::NewStub(channel_);
  return SlurmxErr::kOk;
}

SlurmxErr SqueueXClient::LoadJobByJobId(JobInfoMsg *job_info_ptr,
                                        uint32_t job_id, uint16_t show_flags) {
  SlurmxErr err = SlurmxErr::kOk;

  // Data we are sending to the server.
  SqueueXRequest request;
  request.set_type(SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_INFO_SINGLE);
  request.set_job_id(job_id);
  request.set_show_flags(show_flags);

  SqueueXReply reply;
  // issue RPC to get job information for one job ID
  Status status = stub_->LoadJobs(&context, request, &reply);

  if (status.ok()) {
    if (reply.ok()) {
      job_info_ptr->last_update = reply.job_records().update_time();
      job_info_t job;
      auto job_msg = reply.job_records().job_infos(0);
      job.job_id = job_msg.job_id();
      job.job_name = job_msg.job_name();
      job.user_id = job_msg.user_id();
      job.user_name = job_msg.user_name();
      job.state_desc = job_msg.state_desc();

      job.alloc_res.cpu_count = job_msg.job_resources().cpu_core_limit();
      job.alloc_res.memory_bytes = job_msg.job_resources().memory_limit_bytes();
      job.alloc_res.memory_sw_bytes =
          job_msg.job_resources().memory_sw_limit_bytes();

      job_info_ptr->job_array.push_back(job);
    } else {
      SLURMX_ERROR("Failed to get info. Reason from CtlXd: {}", reply.reason());
      return SlurmxErr::kGenericFailure;
    }
  } else {
    SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                 status.error_message());
    return SlurmxErr::kRpcFailure;
  }
  return err;
}

SlurmxErr SqueueXClient::LoadJobs(absl::Time update_time, JobInfoMsg *job_info_ptr,
                                  uint16_t show_flags) {
  SlurmxErr err = SlurmxErr::kOk;

  std::string time_str = absl::FormatTime(update_time);

  // Data we are sending to the server.
  SqueueXRequest request;
  request.set_type(SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_INFO);
  request.set_update_time(time_str);
  request.set_show_flags(show_flags);

  SqueueXReply reply;

  Status status = stub_->LoadJobs(&context, request, &reply);

  if (status.ok()) {
    if (reply.ok()) {
      job_info_ptr->last_update = reply.job_records().update_time();
      for (auto job_msg : reply.job_records().job_infos()) {
        job_info_t job;

        job.job_id = job_msg.job_id();
        job.job_name = job_msg.job_name();
        job.user_id = job_msg.user_id();
        job.user_name = job_msg.user_name();
        job.state_desc = job_msg.state_desc();

        job.alloc_res.cpu_count = job_msg.job_resources().cpu_core_limit();
        job.alloc_res.memory_bytes =
            job_msg.job_resources().memory_limit_bytes();
        job.alloc_res.memory_sw_bytes =
            job_msg.job_resources().memory_sw_limit_bytes();

        job_info_ptr->job_array.push_back(job);
      }
    } else if (reply.reason() == "JobInfoNoChange") {
      SLURMX_INFO("Job information is not updated.");
      // job infos have no change, use the old job info records. this would be
      // used at --iterate option.
    } else {
      SLURMX_ERROR("Failed to get info. Reason from CtlXd: {}", reply.reason());
      return SlurmxErr::kGenericFailure;
    }
  } else {
    SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                 status.error_message());
    return SlurmxErr::kRpcFailure;
  }
  return err;
}

SlurmxErr SqueueXClient::LoadJobsByUserId(JobInfoMsg *job_info_ptr,
                                          uint32_t user_id,
                                          uint16_t show_flags) {
  SlurmxErr err = SlurmxErr::kOk;

  SqueueXRequest request;
  request.set_type(SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_USER_INFO);
  request.set_user_id(user_id);
  request.set_show_flags(show_flags);

  SqueueXReply reply;

  Status status = stub_->LoadJobs(&context, request, &reply);

  if (status.ok()) {
    if (reply.ok()) {
      job_info_ptr->last_update = reply.job_records().update_time();
      for (auto job_msg : reply.job_records().job_infos()) {
        job_info_t job;

        job.job_id = job_msg.job_id();
        job.job_name = job_msg.job_name();
        job.user_id = job_msg.user_id();
        job.user_name = job_msg.user_name();
        job.state_desc = job_msg.state_desc();

        job.alloc_res.cpu_count = job_msg.job_resources().cpu_core_limit();
        job.alloc_res.memory_bytes =
            job_msg.job_resources().memory_limit_bytes();
        job.alloc_res.memory_sw_bytes =
            job_msg.job_resources().memory_sw_limit_bytes();

        job_info_ptr->job_array.push_back(job);
      }
    } else {
      SLURMX_ERROR("Failed to get info. Reason from CtlXd: {}", reply.reason());
      return SlurmxErr::kGenericFailure;
    }
  } else {
    SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                 status.error_message());
    return SlurmxErr::kRpcFailure;
  }
  return err;
}

SlurmxErr SqueueXClient::PrintJobArray(const JobInfoMsg &jobs) {
  fmt::print("  JOBID   JOBNAME   USERID  USERNAME    STATEDESC\n");
  for (auto job : jobs.job_array) {
    fmt::print("{:7}{: >10}{:9}{: >10}{: >13}", job.job_id, job.job_name,
               job.user_id, job.user_name, job.state_desc);
  }
  return SlurmxErr::kOk;
}

}  // namespace SqueueX
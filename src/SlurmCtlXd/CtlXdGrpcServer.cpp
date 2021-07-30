#include "CtlXdGrpcServer.h"

#include <signal.h>

#include <limits>

#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/String.h"

namespace CtlXd {

grpc::Status CtlXd::SlurmCtlXdServiceImpl::RegisterSlurmXd(
    grpc::ServerContext *context,
    const SlurmxGrpc::SlurmXdRegisterRequest *request,
    SlurmxGrpc::SlurmXdRegisterResult *response) {
  std::string peer = context->peer();
  std::vector<std::string> peer_slices;
  // ["ipv4", "<address>, "<port>"]
  boost::algorithm::split(peer_slices, peer, boost::is_any_of(":"));

  std::string addr_port = fmt::format("{}:{}", peer_slices[1], request->port());

  XdNodeId node_id;
  node_id.partition_id =
      g_meta_container->GetPartitionId(request->partition_name());
  node_id.node_index =
      g_meta_container->AllocNodeIndexInPartition(node_id.partition_id);

  std::future<RegisterNodeResult> result_future = g_node_keeper->RegisterXdNode(
      addr_port, node_id,
      new XdNodeStaticMeta{.node_index = node_id.node_index,
                           .ipv4_addr = peer_slices[1],
                           .port = request->port(),
                           .node_name = request->node_name(),
                           .partition_id = node_id.partition_id,
                           .partition_name = request->partition_name(),
                           .res = request->resource_total()},
      [](void *data) { delete reinterpret_cast<resource_t *>(data); });

  RegisterNodeResult result = result_future.get();

  if (result.node_id.has_value()) {
    response->set_ok(true);
    response->mutable_node_id()->set_partition_id(
        result.node_id.value().partition_id);
    response->mutable_node_id()->set_node_index(
        result.node_id.value().node_index);
  } else {
    response->set_ok(false);
    response->set_reason("CtlXd cannot connect to Xd backward");

    SLURMX_TRACE("Failed to establish the backward channel to XdClient {}.",
                 addr_port);

    // Avoid memory leak.
    g_meta_container->TryReleasePartition(node_id.partition_id);
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AllocateResource(
    grpc::ServerContext *context,
    const SlurmxGrpc::ResourceAllocRequest *request,
    SlurmxGrpc::ResourceAllocReply *response) {
  resource_t res{request->required_resource()};

  SlurmxErr err;
  SlurmxGrpc::ResourceInfo res_info;
  err = m_ctlxd_server_->AllocateResource(request->partition_name(), res,
                                          &res_info);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    *response->mutable_res_info() = res_info;
  } else {
    response->set_ok(false);
    response->set_reason(err == SlurmxErr::kNonExistent
                             ? "Partition doesn't exist!"
                             : "Resource not enough!");
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::DeallocateResource(
    grpc::ServerContext *context,
    const SlurmxGrpc::DeallocateResourceRequest *request,
    SlurmxGrpc::DeallocateResourceReply *response) {
  SlurmxErr err;
  uuid res_uuid;

  std::copy(request->resource_uuid().begin(), request->resource_uuid().end(),
            res_uuid.data);
  err = m_ctlxd_server_->DeallocateResource(
      XdNodeId{request->node_id().partition_id(),
               request->node_id().node_index()},
      res_uuid);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(
        "Node index or resource uuid does not exist. Resource Deallocation "
        "failed.");
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::Heartbeat(
    grpc::ServerContext *context, const SlurmxGrpc::HeartbeatRequest *request,
    SlurmxGrpc::HeartbeatReply *response) {
  uuid node_uuid;
  std::copy(request->node_uuid().begin(), request->node_uuid().end(),
            node_uuid.data);

  m_ctlxd_server_->HeartBeatFromNode(node_uuid);

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::LoadJobs(
    grpc::ServerContext *context, const SlurmxGrpc::SqueueXRequest *request,
    SlurmxGrpc::SqueueXReply *response) {
  SlurmxErr err;
  JobInfoMsg job_infos;
  // fill the response message depending on the request message type.
  if (request->type() ==
      SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_INFO_SINGLE) {
    err = m_ctlxd_server_->GetJobInfoByJobId(&job_infos, request->job_id(),
                                             request->show_flags());
    if (err != SlurmxErr::kOk) {
      SLURMX_ERROR("Failed to load job infos", SlurmxErrStr(err));
      response->set_ok(false);
      response->set_reason(SlurmxErrStr(err).data());
    } else {
      response->set_ok(true);
      // fill the response message from job_infos, it should contain only 1 msg.
      auto *jobs = response->mutable_job_records();
      jobs->set_update_time(static_cast<uint64_t>(time(NULL)));
      auto *infos = jobs->add_job_infos();

      auto job = job_infos.job_array[0];

      infos->set_job_id(job.job_id);
      infos->set_user_name(job.user_name);
      infos->set_user_id(job.user_id);
      infos->set_state_desc(job.state_desc);
      infos->set_job_name(job.job_name);

      auto *alloc_resource = infos->mutable_job_resources();
      alloc_resource->set_cpu_core_limit(job.alloc_res.cpu_count);
      alloc_resource->set_memory_limit_bytes(job.alloc_res.memory_bytes);
      alloc_resource->set_memory_sw_limit_bytes(job.alloc_res.memory_sw_bytes);
    }
  }

  else if (request->type() ==
           SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_USER_INFO) {
    err = m_ctlxd_server_->GetJobInfoByUserId(&job_infos, request->user_id(),
                                              request->show_flags());
    if (err != SlurmxErr::kOk) {
      SLURMX_ERROR("Failed to load job infos", SlurmxErrStr(err));
      response->set_ok(false);
      response->set_reason(SlurmxErrStr(err).data());
    } else {
      response->set_ok(true);
      // reply job info of the user id.
      auto *jobs = response->mutable_job_records();
      jobs->set_update_time(static_cast<uint64_t>(time(NULL)));
      auto *infos = jobs->add_job_infos();

      for (auto job : job_infos.job_array) {
        infos->set_job_id(job.job_id);
        infos->set_user_name(job.user_name);
        infos->set_user_id(job.user_id);
        infos->set_state_desc(job.state_desc);
        infos->set_job_name(job.job_name);

        auto *alloc_resource = infos->mutable_job_resources();
        alloc_resource->set_cpu_core_limit(job.alloc_res.cpu_count);
        alloc_resource->set_memory_limit_bytes(job.alloc_res.memory_bytes);
        alloc_resource->set_memory_sw_limit_bytes(
            job.alloc_res.memory_sw_bytes);
      }
    }
  }

  else if (request->type() ==
           SlurmxGrpc::SqueueXRequest_Type_REQUEST_JOB_INFO) {
    // todo fill job change time
    // local last_update_time of the job record will be compared with remote
    // last_update_time.
    absl::Time local_update_time;
    std::string time_str = request->update_time();
    absl::ParseTime("%Y-%m-%d %H:%M:%S %z", time_str, &local_update_time, nullptr);

    absl::Time job_change_time = local_update_time + absl::Hours(1);
    if (local_update_time > job_change_time) {
      response->set_ok(false);
      response->set_reason((char *)"JobInfoNoChange");
    } else {
      err = m_ctlxd_server_->GetJobsInfo(&job_infos, time_str,
                                         request->show_flags());
      if (err != SlurmxErr::kOk) {
        SLURMX_ERROR("Failed to load job infos", SlurmxErrStr(err));
        response->set_ok(false);
        response->set_reason(SlurmxErrStr(err).data());
      } else {
        // reply job infos.
        response->set_ok(true);
        auto *jobs = response->mutable_job_records();
        jobs->set_update_time(static_cast<uint64_t>(time(NULL)));
        auto *infos = jobs->add_job_infos();

        for (auto job : job_infos.job_array) {
          infos->set_job_id(job.job_id);
          infos->set_user_name(job.user_name);
          infos->set_user_id(job.user_id);
          infos->set_state_desc(job.state_desc);
          infos->set_job_name(job.job_name);

          auto *alloc_ressource = infos->mutable_job_resources();
          alloc_ressource->set_cpu_core_limit(job.alloc_res.cpu_count);
          alloc_ressource->set_memory_limit_bytes(job.alloc_res.memory_bytes);
          alloc_ressource->set_memory_sw_limit_bytes(
              job.alloc_res.memory_sw_bytes);
        }
      }
    }
  }
  return grpc::Status::OK;
}

CtlXdServer::CtlXdServer(const std::string &listen_address)
    : m_listen_address_(listen_address) {
  m_service_impl_ = std::make_unique<SlurmCtlXdServiceImpl>(this);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(m_listen_address_,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  SLURMX_INFO("SlurmCtlXd is listening on {}", m_listen_address_);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread sigint_waiting_thread([p_server = m_server_.get()] {
    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    SLURMX_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtlXdServer::signal_handler_func);

  g_node_keeper->SetNodeIsUpCb(std::bind(&CtlXdServer::XdNodeIsUpCb_, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2));

  g_node_keeper->SetNodeIsDownCb(std::bind(&CtlXdServer::XdNodeIsDownCb_, this,
                                           std::placeholders::_1,
                                           std::placeholders::_2));
}

void CtlXdServer::XdNodeIsUpCb_(XdNodeId node_id, void *node_data) {
  SLURMX_TRACE(
      "A new node #{} is up now. Add its resource to the global resource pool.",
      node_id);

  XdNodeStub *xd_stub = g_node_keeper->GetXdStub(node_id);
  SLURMX_ASSERT(xd_stub != nullptr,
                "Got nullptr of XdNodeStub in NodeIsUp() callback!");

  auto *static_meta = reinterpret_cast<XdNodeStaticMeta *>(node_data);

  g_meta_container->AddNode(*static_meta);

  SLURMX_INFO("Node {} registered. cpu: {}, mem: {}, mem+sw: {}", node_id,
              static_meta->res.cpu_count,
              slurmx::ReadableMemory(static_meta->res.memory_bytes),
              slurmx::ReadableMemory(static_meta->res.memory_sw_bytes));

  // Delete node_data(node_res) (allocated in RegisterSlurmXd) here because it's
  // useless now. The resource information is now kept in global MetaContainer.
  // Set it to nullptr, so next delete call in clean_up_cb will not delete it
  // again.
  xd_stub->SetNodeData(nullptr);
  delete static_meta;
}

void CtlXdServer::XdNodeIsDownCb_(XdNodeId node_id, void *) {
  SLURMX_TRACE(
      "XdNode #{} is down now. Remove its resource from the global resource "
      "pool.",
      node_id);

  g_meta_container->DeleteNodeMeta(node_id);
}

SlurmxErr CtlXdServer::AllocateResource(const std::string &partition_name,
                                        const resource_t &res,
                                        SlurmxGrpc::ResourceInfo *res_info) {
  SLURMX_TRACE("Trying Allocating resource: cpu {}, mem: {}, mem+sw: {}",
               res.cpu_count, slurmx::ReadableMemory(res.memory_bytes),
               slurmx::ReadableMemory(res.memory_sw_bytes));

  if (!g_meta_container->PartitionExists(partition_name)) {
    SLURMX_DEBUG("Partition {} doesn't exist. Resource allocation failed.",
                 partition_name);
    return SlurmxErr::kNonExistent;
  }

  uint32_t partition_id = g_meta_container->GetPartitionId(partition_name);

  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(partition_id);
  if (metas_ptr->partition_global_meta.m_resource_avail_ < res) {
    SLURMX_TRACE(
        "Resource not enough. Avail: cpu {}, mem: {}, mem+sw: {}",
        metas_ptr->partition_global_meta.m_resource_avail_.cpu_count,
        slurmx::ReadableMemory(
            metas_ptr->partition_global_meta.m_resource_avail_.memory_bytes),
        slurmx::ReadableMemory(metas_ptr->partition_global_meta
                                   .m_resource_avail_.memory_sw_bytes));
    return SlurmxErr::kNoResource;
  }

  for (auto &[index, node] : metas_ptr->xd_node_meta_map) {
    bool node_valid = g_node_keeper->XdNodeValid(index);
    SLURMX_TRACE("Node #{} Valid: {}", index, node_valid);
    if (node_valid && res <= node.res_avail) {
      // Todo: We should query the node to test if the required resource
      //  does not exceed the remaining resource on the node.
      //  Slurm seems to work in this way.
      uuid res_uuid;

      res_uuid = m_uuid_gen_();
      node.res_in_use += res;
      node.res_avail -= res;

      node.resource_shards.emplace(res_uuid, res);

      metas_ptr->partition_global_meta.m_resource_in_use_ += res;
      metas_ptr->partition_global_meta.m_resource_avail_ -= res;

      SLURMX_TRACE(
          "Resource allocated successfully. Node index: {}. uuid: {}. "
          "Informing XdClient...",
          index, to_string(res_uuid));

      XdNodeStub *xd_stub =
          g_node_keeper->GetXdStub(XdNodeId{partition_id, index});
      SLURMX_ASSERT(xd_stub != nullptr,
                    "XdNode {}'s stub pointer shouldn't be nullptr when it's "
                    "valid in bitset.");

      res_info->set_node_index(index);
      *res_info->mutable_ipv4_addr() = node.static_meta.ipv4_addr;
      res_info->set_port(node.static_meta.port);
      res_info->mutable_resource_uuid()->assign(res_uuid.begin(),
                                                res_uuid.end());

      return xd_stub->GrantResourceToken(res_uuid, res);
    }
  }

  return SlurmxErr::kNoResource;
}

SlurmxErr CtlXdServer::DeallocateResource(XdNodeId node_id,
                                          const uuid &resource_uuid) {
  SLURMX_TRACE("Trying Deallocating resource uuid in Node {}: {}", node_id,
               boost::uuids::to_string(resource_uuid));
  auto meta_ptr = g_meta_container->GetNodeMetaPtr(node_id);
  if (!meta_ptr) {
    SLURMX_DEBUG("Node {} not found in xd_node_meta_map", node_id);
    return SlurmxErr::kNonExistent;
  }

  XdNodeMeta &node_meta = *meta_ptr;
  auto shard_iter = node_meta.resource_shards.find(resource_uuid);
  if (shard_iter == node_meta.resource_shards.end()) {
    SLURMX_DEBUG("resource uuid {} not found in Node #{}'s shards",
                 boost::uuids::to_string(resource_uuid), node_id);
    return SlurmxErr::kNonExistent;
  }

  // Note: Recursive lock here!
  auto part_metas =
      g_meta_container->GetPartitionMetasPtr(node_id.partition_id);

  // Modify partition meta
  part_metas->partition_global_meta.m_resource_in_use_ -= shard_iter->second;
  part_metas->partition_global_meta.m_resource_avail_ += shard_iter->second;

  // Modify node meta
  node_meta.res_in_use -= shard_iter->second;
  node_meta.res_avail += shard_iter->second;
  node_meta.resource_shards.erase(shard_iter);

  return SlurmxErr::kOk;
}

void CtlXdServer::HeartBeatFromNode(const uuid &node_uuid) {}

// todo fake info
SlurmxErr CtlXdServer::GetJobsInfo(JobInfoMsg *job_info, const std::string &update_time,
                                   uint16_t show_flags) {
  absl::Time now = absl::Now();
  job_info->last_update = absl::FormatTime(now);
  job_info_t job;
  job.job_id = 333;
  job.user_id = 1000;
  job.user_name = (char *)"wzd";
  job.state_desc = (char *)"COMPLETING";
  job.job_name = (char *)"job1";

  job.alloc_res.cpu_count = 2;
  job.alloc_res.memory_bytes = 9999;
  job.alloc_res.memory_sw_bytes = 666;

  job_info->job_array.push_back(job);

  job_info_t job2;
  job2.job_id = 334;
  job2.user_id = 1000;
  job2.user_name = (char *)"wzd";
  job2.state_desc = (char *)"COMPLETING";
  job2.job_name = (char *)"job2";

  job2.alloc_res.cpu_count = 2;
  job2.alloc_res.memory_bytes = 99;
  job2.alloc_res.memory_sw_bytes = 666;

  job_info->job_array.push_back(job2);

  return SlurmxErr::kOk;
}
SlurmxErr CtlXdServer::GetJobInfoByJobId(JobInfoMsg *job_info, uint32_t job_id,
                                         uint16_t show_flags) {
  time_t now;
  time(&now);
  job_info->last_update = now;
  job_info_t job;
  job.job_id = job_id;
  job.user_id = 1000;
  job.user_name = (char *)"wzd";
  job.state_desc = (char *)"COMPLETING";
  job.job_name = (char *)"job1";

  job.alloc_res.cpu_count = 2;
  job.alloc_res.memory_bytes = 9999;
  job.alloc_res.memory_sw_bytes = 666;

  job_info->job_array.push_back(job);

  return SlurmxErr::kOk;
}
SlurmxErr CtlXdServer::GetJobInfoByUserId(JobInfoMsg *job_info,
                                          uint32_t user_id,
                                          uint16_t show_flags) {
  time_t now;
  time(&now);
  job_info->last_update = now;
  job_info_t job;
  job.job_id = 333;
  job.user_id = user_id;
  job.user_name = (char *)"wzd";
  job.state_desc = (char *)"COMPLETING";
  job.job_name = (char *)"job1";

  job.alloc_res.cpu_count = 2;
  job.alloc_res.memory_bytes = 9999;
  job.alloc_res.memory_sw_bytes = 666;

  job_info->job_array.push_back(job);
  return SlurmxErr::kOk;
}

}  // namespace CtlXd
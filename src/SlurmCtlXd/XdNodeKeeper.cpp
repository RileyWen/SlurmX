#include "XdNodeKeeper.h"

#include <google/protobuf/util/time_util.h>

#include <optional>
#include <utility>

namespace CtlXd {

using grpc::ClientContext;
using grpc::Status;

XdNodeStub::XdNodeStub()
    : m_failure_retry_times_(0), m_invalid_(true), m_data_(nullptr) {
  // The most part of jobs are done in XdNodeKeeper::RegisterNewXdNode().
}

XdNodeStub::~XdNodeStub() {
  if (m_clean_up_cb_) m_clean_up_cb_(m_data_);
}

SlurmxErr XdNodeStub::ExecuteTask(const TaskInCtlXd *task) {
  using SlurmxGrpc::ExecuteTaskReply;
  using SlurmxGrpc::ExecuteTaskRequest;

  ExecuteTaskRequest request;
  ExecuteTaskReply reply;
  ClientContext context;
  Status status;

  auto *mutable_task = request.mutable_task();

  // Set time_limit
  mutable_task->mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(task->time_limit)));

  // Set resources
  auto *mutable_allocatable_resource =
      mutable_task->mutable_resources()->mutable_allocatable_resource();
  mutable_allocatable_resource->set_cpu_core_limit(
      task->resources.allocatable_resource.cpu_count);
  mutable_allocatable_resource->set_memory_limit_bytes(
      task->resources.allocatable_resource.memory_bytes);
  mutable_allocatable_resource->set_memory_sw_limit_bytes(
      task->resources.allocatable_resource.memory_sw_bytes);

  // Set type
  mutable_task->set_type(task->type);

  mutable_task->set_task_id(task->task_id);
  mutable_task->set_partition_id(task->partition_id);

  mutable_task->set_node_num(task->node_num);
  mutable_task->set_task_per_node(task->task_per_node);

  mutable_task->mutable_start_time()->set_seconds(
      ToUnixSeconds(task->start_time));
  mutable_task->mutable_time_limit()->set_seconds(
      ToInt64Seconds(task->time_limit));

  if (task->type == SlurmxGrpc::Interactive) {
    auto *mutable_meta = request.mutable_task()->mutable_interactive_meta();

    auto &meta_in_ctlxd = std::get<InteractiveMetaInTask>(task->meta);
    mutable_meta->set_resource_uuid(meta_in_ctlxd.resource_uuid.data,
                                    meta_in_ctlxd.resource_uuid.size());
  } else if (task->type == SlurmxGrpc::Batch) {
    auto &meta_in_ctlxd = std::get<BatchMetaInTask>(task->meta);
    auto *mutable_meta = request.mutable_task()->mutable_batch_meta();
    mutable_meta->set_output_file_pattern(meta_in_ctlxd.output_file_pattern);
    mutable_meta->set_sh_script(meta_in_ctlxd.sh_script);
  }

  status = m_stub_->ExecuteTask(&context, request, &reply);
  if (!status.ok()) {
    SLURMX_DEBUG("Execute RPC for Node {} returned with status not ok: {}",
                 m_node_id_, status.error_message());
    return SlurmxErr::kRpcFailure;
  }

  return SlurmxErr::kOk;
}

SlurmxErr XdNodeStub::TerminateTask(uint32_t task_id) {
  using SlurmxGrpc::TerminateTaskReply;
  using SlurmxGrpc::TerminateTaskRequest;

  ClientContext context;
  Status status;
  TerminateTaskRequest request;
  TerminateTaskReply reply;

  request.set_task_id(task_id);

  status = m_stub_->TerminateTask(&context, request, &reply);
  if (!status.ok()) {
    SLURMX_DEBUG(
        "TerminateTask RPC for Node {} returned with status not ok: {}",
        m_node_id_, status.error_message());
    return SlurmxErr::kRpcFailure;
  }

  if (reply.ok())
    return SlurmxErr::kOk;
  else
    return SlurmxErr::kGenericFailure;
}

XdNodeKeeper::XdNodeKeeper() : m_cq_closed_(false), m_tag_pool_(32, 0) {
  m_cq_thread_ = std::thread(&XdNodeKeeper::StateMonitorThreadFunc_, this);
}

XdNodeKeeper::~XdNodeKeeper() {
  m_cq_mtx_.Lock();

  m_cq_.Shutdown();
  m_cq_closed_ = true;

  m_cq_mtx_.Unlock();

  m_cq_thread_.join();

  // Dependency order: rpc_cq -> channel_state_cq -> tag pool.
  // Tag pool's destructor will free all trailing tags in cq.
}

std::future<RegisterNodeResult> XdNodeKeeper::RegisterXdNode(
    const std::string &node_addr, XdNodeId node_id, void *data,
    std::function<void(void *)> clean_up_cb) {
  using namespace std::chrono_literals;

  auto *cq_tag_data = new InitializingXdTagData{};
  cq_tag_data->xd = std::make_unique<XdNodeStub>();

  // InitializingXd: BEGIN -> IDLE

  cq_tag_data->xd->m_node_id_ = node_id;
  cq_tag_data->xd->m_data_ = data;
  cq_tag_data->xd->m_clean_up_cb_ = std::move(clean_up_cb);

  /* Todo: Adjust the value here.
   * In default case, TRANSIENT_FAILURE -> TRANSIENT_FAILURE will use the
   * connection-backoff algorithm. We might need to adjust these values.
   * https://grpc.github.io/grpc/cpp/md_doc_connection-backoff.html
   */
  grpc::ChannelArguments channel_args;
  //  channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100 /*ms*/);
  //  channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1 /*s*/ * 1000
  //  /*ms*/); channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 2 /*s*/ *
  //  1000 /*ms*/);
  //  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 5 /*s*/ * 1000 /*ms*/);
  //  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10 /*s*/ * 1000
  //  /*ms*/); channel_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1
  //  /*true*/);

  cq_tag_data->xd->m_channel_ = grpc::CreateCustomChannel(
      node_addr, grpc::InsecureChannelCredentials(), channel_args);
  cq_tag_data->xd->m_prev_channel_state_ =
      cq_tag_data->xd->m_channel_->GetState(true);
  cq_tag_data->xd->m_stub_ =
      SlurmxGrpc::SlurmXd::NewStub(cq_tag_data->xd->m_channel_);

  cq_tag_data->xd->m_maximum_retry_times_ = 4;

  CqTag *tag;
  {
    util::lock_guard lock(m_tag_pool_mtx_);
    tag = m_tag_pool_.construct(CqTag{CqTag::kInitializingXd, cq_tag_data});
  }

  // future must be retrieved here previous to NotifyOnStateChange!
  // Otherwise, data may be freed previous to get_future().
  auto result_future = cq_tag_data->register_result.get_future();

  cq_tag_data->xd->m_channel_->NotifyOnStateChange(
      cq_tag_data->xd->m_prev_channel_state_,
      std::chrono::system_clock::now() + 2s, &m_cq_, tag);

  return result_future;
}

void XdNodeKeeper::StateMonitorThreadFunc_() {
  using namespace std::chrono_literals;

  bool ok;
  CqTag *tag;

  while (true) {
    if (m_cq_.Next((void **)&tag, &ok)) {
      XdNodeStub *xd;
      switch (tag->type) {
        case CqTag::kInitializingXd:
          xd = reinterpret_cast<InitializingXdTagData *>(tag->data)->xd.get();
          break;
        case CqTag::kEstablishedXd:
          xd = reinterpret_cast<XdNodeStub *>(tag->data);
          break;
      }
      // SLURMX_TRACE("CQ: ok: {}, tag: {}, xd: {}, prev state: {}", ok,
      // tag->type, (void *)xd, xd->m_prev_channel_state_);

      if (ok) {
        CqTag *next_tag = nullptr;
        grpc_connectivity_state new_state = xd->m_channel_->GetState(true);

        switch (tag->type) {
          case CqTag::kInitializingXd:
            next_tag = InitXdStateMachine_((InitializingXdTagData *)tag->data,
                                           new_state);
            break;
          case CqTag::kEstablishedXd:
            next_tag = EstablishedXdStateMachine_(xd, new_state);
            break;
        }
        if (next_tag) {
          util::lock_guard lock(m_cq_mtx_);
          if (!m_cq_closed_) {
            // SLURMX_TRACE("Registering next tag: {}", next_tag->type);

            xd->m_prev_channel_state_ = new_state;
            // When cq is closed, do not register any more callbacks on it.
            xd->m_channel_->NotifyOnStateChange(
                xd->m_prev_channel_state_,
                std::chrono::system_clock::now() + 3s, &m_cq_, next_tag);
          }
        } else {
          // END state of both state machine. Free the Xd client.
          if (tag->type == CqTag::kInitializingXd) {
            // Set future of RegisterNodeResult and free tag_data
            SLURMX_TRACE("Set future to false");
            auto *tag_data =
                reinterpret_cast<InitializingXdTagData *>(tag->data);
            tag_data->register_result.set_value({std::nullopt});
            delete tag_data;
          } else if (tag->type == CqTag::kEstablishedXd) {
            if (m_node_is_down_cb_)
              m_node_is_down_cb_(xd->m_node_id_, xd->m_data_);

            util::lock_guard node_lock(m_node_mtx_);
            util::write_lock_guard xd_lock(m_alive_xd_rw_mtx_);

            m_empty_slot_bitset_[xd->m_slot_offset_] = true;
            m_alive_xd_bitset_[xd->m_slot_offset_] = false;

            m_node_id_slot_offset_map_.erase(xd->m_node_id_);

            m_node_vec_[xd->m_slot_offset_].reset();
          } else {
            SLURMX_ERROR("Unknown tag type: {}", tag->type);
          }
        }

        util::lock_guard lock(m_tag_pool_mtx_);
        m_tag_pool_.free(tag);
      } else {
        /* ok = false implies that NotifyOnStateChange() timed out.
         * See GRPC code: src/core/ext/filters/client_channel/
         *  channel_connectivity.cc:grpc_channel_watch_connectivity_state()
         *
         * Register the same tag again. Do not free it because we have no newly
         * allocated tag. */
        util::lock_guard lock(m_cq_mtx_);
        if (!m_cq_closed_) {
          SLURMX_TRACE("Registering next tag: {}", tag->type);

          // When cq is closed, do not register any more callbacks on it.
          xd->m_channel_->NotifyOnStateChange(
              xd->m_prev_channel_state_, std::chrono::system_clock::now() + 3s,
              &m_cq_, tag);
        }
      }
    } else {
      // m_cq_.Shutdown() has been called. Exit the thread.
      break;
    }
  }
}

XdNodeKeeper::CqTag *XdNodeKeeper::InitXdStateMachine_(
    InitializingXdTagData *tag_data, grpc_connectivity_state new_state) {
  SLURMX_TRACE("Enter InitXdStateMachine_");

  std::optional<CqTag::Type> next_tag_type;
  XdNodeStub *raw_xd = tag_data->xd.get();

  switch (new_state) {
    case GRPC_CHANNEL_READY: {
      {
        SLURMX_TRACE("CONNECTING -> READY");
        // The two should be modified as a whole.
        util::lock_guard node_lock(m_node_mtx_);
        util::write_lock_guard xd_w_lock(m_alive_xd_rw_mtx_);

        size_t pos = m_empty_slot_bitset_.find_first();
        if (pos == boost::dynamic_bitset<>::npos) {
          // No more room for new elements.
          raw_xd->m_slot_offset_ = m_empty_slot_bitset_.size();

          SLURMX_TRACE("Append Xd at new slot #{}", raw_xd->m_slot_offset_);

          // Transfer the ownership of this XdNodeStub to smart pointer.
          m_node_vec_.emplace_back(std::move(tag_data->xd));

          m_empty_slot_bitset_.push_back(false);
          m_alive_xd_bitset_.push_back(true);
        } else {
          SLURMX_TRACE("Insert Xd at empty slot #{}", pos);
          // Find empty slot.
          raw_xd->m_slot_offset_ = pos;

          // Transfer the XdNodeStub ownership.
          m_node_vec_[pos] = std::move(tag_data->xd);
          m_empty_slot_bitset_[pos] = false;
          m_alive_xd_bitset_[pos] = true;
        }

        m_node_id_slot_offset_map_.emplace(raw_xd->m_node_id_,
                                           raw_xd->m_slot_offset_);

        raw_xd->m_failure_retry_times_ = 0;
        raw_xd->m_invalid_ = false;
      }
      if (m_node_is_up_cb_)
        m_node_is_up_cb_(raw_xd->m_node_id_, raw_xd->m_data_);

      // Set future of RegisterNodeResult and free tag_data
      tag_data->register_result.set_value({raw_xd->m_node_id_});
      delete tag_data;

      // Switch to EstablishedXd state machine
      next_tag_type = CqTag::kEstablishedXd;
      break;
    }

    case GRPC_CHANNEL_TRANSIENT_FAILURE: {
      if (raw_xd->m_failure_retry_times_ < raw_xd->m_maximum_retry_times_) {
        raw_xd->m_failure_retry_times_++;
        next_tag_type = CqTag::kInitializingXd;

        SLURMX_TRACE(
            "CONNECTING/TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> CONNECTING");
        // prev                            current              next
        // CONNECTING/TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> CONNECTING
      } else {
        next_tag_type = std::nullopt;
        SLURMX_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END");
        // prev must be TRANSIENT_FAILURE.
        // when prev is CONNECTING, retry_times = 0
        // prev          current              next
        // TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END
      }
      break;
    }

    case GRPC_CHANNEL_CONNECTING: {
      if (raw_xd->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (raw_xd->m_failure_retry_times_ < raw_xd->m_maximum_retry_times_) {
          // prev          current
          // CONNECTING -> CONNECTING (Timeout)
          SLURMX_TRACE("CONNECTING -> CONNECTING");
          raw_xd->m_failure_retry_times_++;
          next_tag_type = CqTag::kInitializingXd;
        } else {
          // prev          current       next
          // CONNECTING -> CONNECTING -> END
          SLURMX_TRACE("CONNECTING -> CONNECTING -> END");
          next_tag_type = std::nullopt;
        }
      } else {
        // prev    now
        // IDLE -> CONNECTING
        SLURMX_TRACE("IDLE -> CONNECTING");
        next_tag_type = CqTag::kInitializingXd;
      }
      break;
    }

    case GRPC_CHANNEL_IDLE:
      // InitializingXd: BEGIN -> IDLE state switching is handled in
      // XdNodeKeeper::RegisterNewXdNode. Execution should never reach here.
      SLURMX_ERROR("Unexpected InitializingXd IDLE state!");
      break;

    case GRPC_CHANNEL_SHUTDOWN:
      SLURMX_ERROR("Unexpected InitializingXd SHUTDOWN state!");
      break;
  }

  SLURMX_TRACE("Exit InitXdStateMachine_");
  if (next_tag_type.has_value()) {
    if (next_tag_type.value() == CqTag::kInitializingXd) {
      util::lock_guard lock(m_tag_pool_mtx_);
      return m_tag_pool_.construct(CqTag{next_tag_type.value(), tag_data});
    } else if (next_tag_type.value() == CqTag::kEstablishedXd) {
      util::lock_guard lock(m_tag_pool_mtx_);
      return m_tag_pool_.construct(CqTag{next_tag_type.value(), raw_xd});
    }
  }
  return nullptr;
}

XdNodeKeeper::CqTag *XdNodeKeeper::EstablishedXdStateMachine_(
    XdNodeStub *xd, grpc_connectivity_state new_state) {
  SLURMX_TRACE("Enter EstablishedXdStateMachine_");

  std::optional<CqTag::Type> next_tag_type;

  switch (new_state) {
    case GRPC_CHANNEL_CONNECTING: {
      if (xd->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (xd->m_failure_retry_times_ < xd->m_maximum_retry_times_) {
          // prev          current
          // CONNECTING -> CONNECTING (Timeout)
          SLURMX_TRACE("CONNECTING -> CONNECTING");
          xd->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedXd;
        } else {
          // prev          current       next
          // CONNECTING -> CONNECTING -> END
          SLURMX_TRACE("CONNECTING -> CONNECTING -> END");
          next_tag_type = std::nullopt;
        }
      } else {
        // prev    now
        // IDLE -> CONNECTING
        SLURMX_TRACE("IDLE -> CONNECTING");
        next_tag_type = CqTag::kEstablishedXd;
      }
      break;
    }

    case GRPC_CHANNEL_IDLE: {
      // prev     current
      // READY -> IDLE (the only edge)
      SLURMX_TRACE("READY -> IDLE");

      xd->m_invalid_ = true;
      {
        util::write_lock_guard lock(m_alive_xd_rw_mtx_);
        m_alive_xd_bitset_[xd->m_slot_offset_] = false;
      }
      if (m_node_is_temp_down_cb_)
        m_node_is_temp_down_cb_(xd->m_node_id_, xd->m_data_);

      next_tag_type = CqTag::kEstablishedXd;
      break;
    }

    case GRPC_CHANNEL_READY: {
      if (xd->m_prev_channel_state_ == GRPC_CHANNEL_READY) {
        // READY -> READY
        SLURMX_TRACE("READY -> READY");
        next_tag_type = CqTag::kEstablishedXd;
      } else {
        // prev          current
        // CONNECTING -> READY
        SLURMX_TRACE("CONNECTING -> READY");

        xd->m_failure_retry_times_ = 0;
        xd->m_invalid_ = false;
        {
          util::write_lock_guard lock(m_alive_xd_rw_mtx_);
          m_alive_xd_bitset_[xd->m_slot_offset_] = true;
        }

        if (m_node_rec_from_temp_failure_cb_)
          m_node_rec_from_temp_failure_cb_(xd->m_node_id_, xd->m_data_);

        next_tag_type = CqTag::kEstablishedXd;
      }
      break;
    }

    case GRPC_CHANNEL_TRANSIENT_FAILURE: {
      if (xd->m_prev_channel_state_ == GRPC_CHANNEL_READY) {
        // prev     current              next
        // READY -> TRANSIENT_FAILURE -> CONNECTING
        SLURMX_TRACE("READY -> TRANSIENT_FAILURE -> CONNECTING");

        xd->m_invalid_ = true;
        {
          util::write_lock_guard lock(m_alive_xd_rw_mtx_);
          m_alive_xd_bitset_[xd->m_slot_offset_] = false;
        }
        if (m_node_is_temp_down_cb_)
          m_node_is_temp_down_cb_(xd->m_node_id_, xd->m_data_);

        next_tag_type = CqTag::kEstablishedXd;
      } else if (xd->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (xd->m_failure_retry_times_ < xd->m_maximum_retry_times_) {
          // prev          current              next
          // CONNECTING -> TRANSIENT_FAILURE -> CONNECTING
          SLURMX_TRACE("CONNECTING -> TRANSIENT_FAILURE -> CONNECTING ({}/{})",
                       xd->m_failure_retry_times_, xd->m_maximum_retry_times_);

          xd->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedXd;
        } else {
          // prev          current              next
          // CONNECTING -> TRANSIENT_FAILURE -> END
          SLURMX_TRACE("CONNECTING -> TRANSIENT_FAILURE -> END");
          next_tag_type = std::nullopt;
        }
      } else if (xd->m_prev_channel_state_ == GRPC_CHANNEL_TRANSIENT_FAILURE) {
        if (xd->m_failure_retry_times_ < xd->m_maximum_retry_times_) {
          // prev                 current
          // TRANSIENT_FAILURE -> TRANSIENT_FAILURE (Timeout)
          SLURMX_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE ({}/{})",
                       xd->m_failure_retry_times_, xd->m_maximum_retry_times_);
          xd->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedXd;
        } else {
          // prev                 current       next
          // TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END
          SLURMX_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END");
          next_tag_type = std::nullopt;
        }
      } else {
        SLURMX_ERROR("Unknown State: {} -> TRANSIENT_FAILURE",
                     xd->m_prev_channel_state_);
      }
      break;
    }

    case GRPC_CHANNEL_SHUTDOWN:
      SLURMX_ERROR("Unexpected SHUTDOWN channel state on EstablishedXd #{} !",
                   xd->m_slot_offset_);
      break;
  }

  if (next_tag_type.has_value()) {
    util::lock_guard lock(m_tag_pool_mtx_);
    SLURMX_TRACE("Exit EstablishedXdStateMachine_");
    return m_tag_pool_.construct(CqTag{next_tag_type.value(), xd});
  }

  SLURMX_TRACE("Exit EstablishedXdStateMachine_");
  return nullptr;
}

uint32_t XdNodeKeeper::AvailableNodeCount() {
  util::read_lock_guard r_lock(m_alive_xd_rw_mtx_);
  return m_alive_xd_bitset_.count();
}

XdNodeStub *XdNodeKeeper::GetXdStub(XdNodeId node_id) {
  util::lock_guard lock(m_node_mtx_);
  auto iter = m_node_id_slot_offset_map_.find(node_id);
  if (iter != m_node_id_slot_offset_map_.end())
    return m_node_vec_[iter->second].get();
  else
    return nullptr;
}

bool XdNodeKeeper::XdNodeValid(uint32_t index) {
  util::read_lock_guard r_lock(m_alive_xd_rw_mtx_);

  return m_alive_xd_bitset_.test(index);
}

void XdNodeKeeper::SetNodeIsUpCb(std::function<void(XdNodeId, void *)> cb) {
  m_node_is_up_cb_ = std::move(cb);
}

void XdNodeKeeper::SetNodeIsDownCb(std::function<void(XdNodeId, void *)> cb) {
  m_node_is_down_cb_ = std::move(cb);
}

void XdNodeKeeper::SetNodeIsTempDownCb(
    std::function<void(XdNodeId, void *)> cb) {
  m_node_is_temp_down_cb_ = std::move(cb);
}

void XdNodeKeeper::SetNodeRecFromTempFailureCb(
    std::function<void(XdNodeId, void *)> cb) {
  m_node_rec_from_temp_failure_cb_ = std::move(cb);
}

}  // namespace CtlXd
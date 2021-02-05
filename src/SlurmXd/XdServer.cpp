#include "XdServer.h"

namespace SlurmXd {

Status SlurmXdServiceImpl::SrunXStream(
    ServerContext *context,
    ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest> *stream) {
  SLURMX_DEBUG("SrunX connects from {}", context->peer());

  enum class StreamState {
    NEGOTIATION = 0,
    NEW_TASK,
    WAIT_FOR_EOF_OR_SIG,
    ABORT
  };

  bool ok;
  SrunXStreamRequest request;
  SrunXStreamReply reply;

  StreamState state = StreamState::NEGOTIATION;
  while (true) {
    switch (state) {
      case StreamState::NEGOTIATION:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() !=
              slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
            SLURMX_DEBUG("Expect negotiation from peer {}, but none.",
                         context->peer());
            state = StreamState::ABORT;
          } else {
            state = StreamState::NEW_TASK;
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::ABORT;
        }
        break;

      case StreamState::NEW_TASK:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
            SLURMX_DEBUG("Expect new_task from peer {}, but none.",
                         context->peer());
            state = StreamState::ABORT;
          } else {
            state = StreamState::WAIT_FOR_EOF_OR_SIG;
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::ABORT;
        }
        break;

      case StreamState::WAIT_FOR_EOF_OR_SIG:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_Signal) {
            SLURMX_DEBUG("Expect signal from peer {}, but none.",
                         context->peer());
            state = StreamState::ABORT;
          } else {
            if (request.signal().signal_type() ==
                slurmx_grpc::Signal_SignalType_Interrupt) {
              // Todo: Send signal to task manager here and wait for result.

              slurmx_grpc::TaskExitStatus *exit_status =
                  reply.mutable_task_exit_status();
              exit_status->set_return_value(9);
              exit_status->set_reason("SIGKILL");

              reply.set_allocated_task_exit_status(exit_status);
              stream->WriteLast(reply, grpc::WriteOptions());

              return Status::OK;
            }
            return Status::OK;
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::ABORT;
        }
        break;

      case StreamState::ABORT:
        SLURMX_DEBUG("Connection from peer {} aborted.", context->peer());
        return Status::CANCELLED;

      default:
        SLURMX_ERROR("Unexpected XdServer State: {}", state);
        return Status::CANCELLED;
    }
  }
}

}  // namespace SlurmXd

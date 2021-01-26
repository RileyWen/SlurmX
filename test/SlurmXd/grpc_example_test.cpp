#include <grpc++/grpc++.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <queue>
#include <thread>

#include "gtest/gtest.h"
#include "protos/grpc_example.grpc.pb.h"
#include "protos/grpc_example.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using grpc_example::Greeter;
using grpc_example::HelloReply;
using grpc_example::HelloRequest;

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }
};

TEST(GrpcExample, Simple) {
  std::string server_address("localhost:50051");
  GreeterServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  std::string user("world");
  std::string reply = greeter.SayHello(user);
  spdlog::info("Greeter received: {}", reply);

  EXPECT_EQ(reply, "Hello world");

  // This method is thread-safe.
  server->Shutdown();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

using grpc::CompletionQueue;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerCompletionQueue;
using grpc_example::Math;
using grpc_example::MaxRequest;
using grpc_example::MaxResponse;

// NOTE: This is a complex example for an asynchronous, bidirectional streaming
// server.

// Most of the logic is similar to AsyncBidiGreeterClient, so follow that class
// for detailed comments. Two main differences between the server and the client
// are: (a) Server cannot initiate a connection, so it first waits for a
// 'connection'. (b) Server can handle multiple streams at the same time, so
// the completion queue/server have a longer lifetime than the client(s).
class AsyncBidiMathServer {
 public:
  AsyncBidiMathServer() {
    // In general avoid setting up the server in the main thread (specifically,
    // in a constructor-like function such as this). We ignore this in the
    // context of an example.
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&m_async_serv_);
    m_server_cq_ = builder.AddCompletionQueue();
    m_server_ = builder.BuildAndStart();

    m_new_call_cq_thread_ = std::make_unique<std::thread>(
        &AsyncBidiMathServer::m_new_call_cq_func_, this);

    m_server_cq_thread_ = std::make_unique<std::thread>(
        &AsyncBidiMathServer::m_server_cq_func_, this);
  }

  void WaitStop() {
    m_server_->Shutdown();

    // Always shutdown the completion queue after the server.
    m_server_cq_->Shutdown();
    m_new_call_cq_.Shutdown();

    m_new_call_cq_thread_->join();
    m_server_cq_thread_->join();
  }

  enum class cq_tag_t : uint8_t {
    NEW_RPC_ESTAB = 0,
    WRITE,
    READ,
    DONE,
  };

 private:
  class ClientConn;

  static constexpr size_t cq_tag_n_bit = 3;

  void m_new_call_cq_func_() {
    spdlog::info("m_new_call_cq_func_ started.");

    // Call RequestMax in the constructor.
    auto new_conn =
        std::make_unique<ClientConn>(&m_async_serv_, &m_new_call_cq_,
                                     m_server_cq_.get(), m_next_client_index_);
    m_next_client_index_++;

    m_client_conns_.emplace(m_next_client_index_ - 1, std::move(new_conn));

    while (true) {
      void* got_tag = nullptr;
      bool ok = false;
      if (!m_new_call_cq_.Next(&got_tag, &ok)) {
        spdlog::info(
            "Completion Queue for new calls closed. Thread for handling new "
            "calls is ending...");
        break;
      }

      if (ok) {
        uint64_t index = index_from_tag(got_tag);
        cq_tag_t status = status_from_tag(got_tag);

        spdlog::info(
            "[NewCallCq] Client {} | Status: {}", index,
            (status == cq_tag_t::READ)
                ? "READ"
                : ((status == cq_tag_t::WRITE)
                       ? "WRITE"
                       : (status == cq_tag_t::NEW_RPC_ESTAB ? "NEW_RPC_ESTAB"
                                                            : "DONE")));

        auto iter = m_client_conns_.find(index);
        if (GPR_UNLIKELY(iter == m_client_conns_.end())) {
          spdlog::error("[NewCallCq] Client {} doesn't exist!", index);
        } else {
          ClientConn* conn = iter->second.get();

          if (status == cq_tag_t::READ) {
            const MaxRequest& req = conn->GetRequest();

            if (req.a() != 0 || req.b() != 0) {
              spdlog::info(
                  "[NewCallCq] Receive Request MAX({},{}) from client {}",
                  req.a(), req.b(), index);

              MaxResponse resp;
              resp.set_result(std::max(req.a(), req.b()));

              conn->RequestWrite(resp);

              // Todo: Here should thread pool to handle request!

              // Request Next Read.
              conn->RequestRead();
            } else {
              conn->EndConn();
            }
          } else if (status == cq_tag_t::WRITE) {
            spdlog::info(
                "[NewCallCq] Write response to Client {} successfully.");
            conn->WriteFinished();
          } else {
            spdlog::error("[NewCallCq] Unexpected status {} of Client {}!",
                          status, index);
          }
        }
      } else {
        spdlog::error("[NewCallCq] server_cq_.Next() returned with ok false!");
      }
    }
  }

  void m_server_cq_func_() {
    spdlog::info("m_server_cq_func_ started.");

    while (true) {
      void* got_tag = nullptr;
      bool ok = false;
      if (!m_server_cq_->Next(&got_tag, &ok)) {
        spdlog::info(
            "[ServerCq] Server Completion Queue closed. Thread for server_cq "
            "is "
            "ending...");
        break;
      }

      if (ok) {
        uint64_t index = index_from_tag(got_tag);
        cq_tag_t status = status_from_tag(got_tag);

        spdlog::info(
            "[ServerCq] Client {} | Status: {}", index,
            (status == cq_tag_t::READ)
                ? "READ"
                : ((status == cq_tag_t::WRITE)
                       ? "WRITE"
                       : (status == cq_tag_t::NEW_RPC_ESTAB ? "NEW_RPC_ESTAB"
                                                            : "DONE")));

        GPR_ASSERT(status == cq_tag_t::WRITE || status == cq_tag_t::READ ||
                   status == cq_tag_t::DONE ||
                   status == cq_tag_t::NEW_RPC_ESTAB);

        auto iter = m_client_conns_.find(index);
        if (GPR_UNLIKELY(iter == m_client_conns_.end())) {
          spdlog::error("[ServerCq] Client {} doesn't exist!", index);
        } else {
          ClientConn* conn = iter->second.get();

          if (status == cq_tag_t::DONE) {
            spdlog::info(
                "[ServerCq] Client {}'s RPC has ended. Removing it...");

            m_client_conns_.erase(index);
          } else if (status == cq_tag_t::NEW_RPC_ESTAB) {
            GPR_ASSERT(index_from_tag(got_tag) == m_next_client_index_ - 1);
            GPR_ASSERT(status_from_tag(got_tag) == cq_tag_t::NEW_RPC_ESTAB);

            spdlog::info(
                "[ServerCq] RPC for client {} established. Requesting read...",
                m_next_client_index_ - 1);
            conn->RequestRead();

            // Prepare next incoming RPC.
            auto new_conn = std::make_unique<ClientConn>(
                &m_async_serv_, &m_new_call_cq_, m_server_cq_.get(),
                m_next_client_index_);
            m_next_client_index_++;

            m_client_conns_.emplace(m_next_client_index_ - 1,
                                    std::move(new_conn));
          } else {
            spdlog::error("[ServerCq] Unexpected status {} of Client {}!",
                          status, index);
          }
        }
      } else {
        spdlog::error("[ServerCq] server_cq_.Next() returned with ok false!");
      }
    }
  }

  Math::AsyncService m_async_serv_ = {};

  CompletionQueue m_new_call_cq_;

  std::unique_ptr<ServerCompletionQueue> m_server_cq_;
  std::unique_ptr<Server> m_server_;

  std::unordered_map<uint64_t, std::unique_ptr<ClientConn>> m_client_conns_;

  std::unique_ptr<std::thread> m_new_call_cq_thread_;
  std::unique_ptr<std::thread> m_server_cq_thread_;

  std::atomic_bool m_should_stop_ = false;

  uint64_t m_next_client_index_ = 0;

 public:
  static enum cq_tag_t status_from_tag(void* tag) {
    constexpr std::uintptr_t zero{};
    constexpr size_t pointer_n_bit = sizeof(std::uintptr_t) * 8;
    constexpr std::uintptr_t status_mask = (~zero)
                                           << (pointer_n_bit - cq_tag_n_bit);
    return static_cast<cq_tag_t>(
        (reinterpret_cast<uintptr_t>(tag) & status_mask) >>
        (pointer_n_bit - cq_tag_n_bit));
  }

  static uint64_t index_from_tag(void* tag) {
    constexpr std::uintptr_t zero{};
    constexpr std::uintptr_t index_mask = (~zero) >> cq_tag_n_bit;
    return reinterpret_cast<uintptr_t>(tag) & index_mask;
  }

  static void* tag_from_index_status(uint64_t index, cq_tag_t status) {
    constexpr std::uintptr_t zero{};
    constexpr size_t pointer_n_bit = sizeof(std::uintptr_t) * 8;
    std::uintptr_t status_part = zero | (static_cast<std::uintptr_t>(status)
                                         << (pointer_n_bit - cq_tag_n_bit));
    std::uintptr_t index_part =
        static_cast<std::uintptr_t>(index) & ((~zero) >> cq_tag_n_bit);

    return reinterpret_cast<void*>(status_part | index_part);
  }

 private:
  class ClientConn {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    ClientConn(Math::AsyncService* service, CompletionQueue* new_call_cq,
               ServerCompletionQueue* server_cq, uint64_t index)
        : m_stream_(&m_rpc_ctx_), m_index_(index) {
      // As part of the initial CREATE state, we *request* that the system
      // start processing SayHello requests. In this request, "this" acts are
      // the tag uniquely identifying the request (so that different CallData
      // instances can serve different requests concurrently), in this case
      // the memory address of this CallData instance.
      service->RequestMax(
          &m_rpc_ctx_, &m_stream_, new_call_cq, server_cq,
          tag_from_index_status(index, cq_tag_t::NEW_RPC_ESTAB));

      // This is important as the server should know when the client is done.
      m_rpc_ctx_.AsyncNotifyWhenDone(
          tag_from_index_status(index, cq_tag_t::DONE));
    }

    void RequestRead() {
      m_stream_.Read(&m_req_, tag_from_index_status(m_index_, cq_tag_t::READ));
    }

    const MaxRequest& GetRequest() const { return m_req_; }

    void RequestWrite(const MaxResponse& resp) {
      if (m_is_writing_) {
        m_write_queue_.push(resp);
      } else {
        m_is_writing_ = true;
        m_stream_.Write(resp, tag_from_index_status(m_index_, cq_tag_t::WRITE));
      }
    }

    void WriteFinished() {
      if (m_write_queue_.empty()) {
        if (m_end_conn_) {
          m_stream_.Finish(Status::OK,
                           tag_from_index_status(m_index_, cq_tag_t::DONE));
        } else {
          m_is_writing_ = false;
        }
      } else {
        m_stream_.Write(m_write_queue_.front(),
                        tag_from_index_status(m_index_, cq_tag_t::WRITE));
        m_write_queue_.pop();
      }
    }

    void EndConn() {
      m_end_conn_ = true;
      if (!m_is_writing_) {
        m_stream_.Finish(Status::OK,
                         tag_from_index_status(m_index_, cq_tag_t::DONE));
      }
    }

   private:
    uint64_t m_index_;

    MaxRequest m_req_;

    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext m_rpc_ctx_;

    // The context, request and m_stream_ are ready once the tag is retrieved
    // from m_cq_->Next().
    ServerAsyncReaderWriter<MaxResponse, MaxRequest> m_stream_;

    std::queue<MaxResponse> m_write_queue_;
    bool m_is_writing_ = false;
    bool m_end_conn_ = false;
  };
};

class BidiMathClient {
 public:
  BidiMathClient(std::shared_ptr<Channel> channel)
      : m_stub_(Math::NewStub(channel)) {
    m_stream_ = m_stub_->Max(&m_ctx_);
    m_recv_thread_ = std::thread(&BidiMathClient::ReceiveRespThread, this);
  }

  ~BidiMathClient() { Wait(); }

  void Wait() {
    if (m_recv_thread_.joinable()) m_recv_thread_.join();
  }

  void ReceiveRespThread() {
    spdlog::info("Client Recv Thread Started...");

    MaxResponse resp;
    while (m_stream_->Read(&resp)) {
      spdlog::info("Client Received the response: {}", resp.result());
    }

    spdlog::info("Client Recv Thread Exiting...");
  }

  void FindMultipleMax(const std::vector<std::pair<int32_t, int32_t>>& pairs) {
    for (auto&& pair : pairs) {
      MaxRequest req;
      req.set_a(pair.first);
      req.set_b(pair.second);
      m_stream_->Write(req);
    }

    m_stream_->Finish();
  }

 private:
  ClientContext m_ctx_;
  std::unique_ptr<grpc::ClientReaderWriter<MaxRequest, MaxResponse>> m_stream_;
  std::unique_ptr<Math::Stub> m_stub_;
  std::thread m_recv_thread_;
};

TEST(GrpcExample, BidirectionalStream) {
  using tag_t = AsyncBidiMathServer::cq_tag_t;

  void* tag = AsyncBidiMathServer::tag_from_index_status(3, tag_t::READ);
  ASSERT_EQ(AsyncBidiMathServer::index_from_tag(tag), 3);
  ASSERT_EQ(AsyncBidiMathServer::status_from_tag(tag), tag_t::READ);

  AsyncBidiMathServer server;

  BidiMathClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));

  std::vector<std::pair<int32_t, int32_t>> reqs{{1, 2},
                                                {3, 4},
                                                {5, 6},
                                                {7, 8},
                                                {0, 0}};

  client.FindMultipleMax(reqs);

  client.Wait();
  server.WaitStop();
}
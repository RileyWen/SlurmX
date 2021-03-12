#include "OptParse.h"
#include "PublicHeader.h"
#include "SrunXClient.h"

std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_;
int main(int argc, char** argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  SlurmxErr err;
  SrunXClient client;
  err = client.Init(argc, const_cast<char**>(argv));
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  }

  err = client.Run();
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  } else
    return 0;
}
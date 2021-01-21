#include <evrpc.h>
#include <spdlog/fmt/bundled/core.h>

#include <random>

#include "AnonymousPipe.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "event2/util.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

static void log_cb(int severity, const char *msg) {
  switch (severity) {
    case _EVENT_LOG_DEBUG:
      spdlog::debug("{}", msg);
      break;
    case _EVENT_LOG_MSG:
      spdlog::info("{}", msg);
      break;
    case _EVENT_LOG_WARN:
      spdlog::warn("{}", msg);
      break;
    case _EVENT_LOG_ERR:
      spdlog::error("{}", msg);
      break;
    default:
      break; /* never reached */
  }
}

class STATIC_FUNC {
  STATIC_FUNC() {
    event_enable_debug_mode();
    event_enable_debug_logging(EVENT_DBG_ALL);
    event_set_log_callback(log_cb);
  }
};

static STATIC_FUNC static_func_();

// NOTICE: The test result indicates that this handler
// must be installed before any fork().
// TODO: Creating child process cause SIGCHLD?
static void sigchld_handler(int sig) {
  spdlog::info("SIGCHLD received too early.");
}

struct EVSketchTest : testing::Test {
  EVSketchTest()
      : m_ev_sigchld_(nullptr), m_ev_base_(nullptr), testing::Test() {}

  void SetUp() override {
    signal(SIGCHLD, sigchld_handler);
    m_ev_base_ = event_base_new();
    if (!m_ev_base_) {
      FAIL() << "Could not initialize libevent!";
    }
  }

  void TearDown() override {
    if (m_ev_sigchld_) event_free(m_ev_sigchld_);
    if (m_ev_base_) event_base_free(m_ev_base_);
  }

  static std::string RandomFileNameStr() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist(100000, 999999);

    return std::to_string(dist(mt));
  }

  static void read_uv_stream(struct bufferevent *bev, void *user_data) {
    auto *this_ = reinterpret_cast<EVSketchTest *>(user_data);

    size_t buf_len = evbuffer_get_length(bev->input);
    spdlog::info("Trying to read buffer(len: {})...", buf_len);
    EXPECT_GT(buf_len, 0);

    char str_[buf_len + 1];
    int n_copy = evbuffer_remove(bev->input, str_, buf_len);
    str_[buf_len] = '\0';
    std::string_view str(str_);

    spdlog::info("Read from child(Copied: {} bytes): {}", n_copy, str);
    EXPECT_EQ(str, this_->m_expected_str_);
  }

  static void sigchld_handler_func(evutil_socket_t sig, short events,
                                   void *user_data) {
    auto *this_ = reinterpret_cast<EVSketchTest *>(user_data);

    spdlog::info("SIGCHLD received...");
    int status;
    wait(&status);

    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 1);

    timeval delay{0, 0};
    event_base_loopexit(this_->m_ev_base_, &delay);
  }

  struct event_base *m_ev_base_;
  event *m_ev_sigchld_;

  std::string m_expected_str_;
};

TEST_F(EVSketchTest, IoRedirectAndDynamicTaskAdding) {
  std::string test_prog_path = "/tmp/slurmxd_test_" + RandomFileNameStr();
  std::string prog_text =
      "#include <iostream>\\n"
      "#include <thread>\\n"
      "#include <chrono>\\n"
      "int main() { std::cout<<\"Hello World!\";"
      //"std::this_thread::sleep_for(std::chrono::seconds(1));"
      "return 1;"
      "}";

  m_expected_str_ = "Hello World!";

  std::string cmd;

  cmd = fmt::format(R"(bash -c 'echo -e '"'"'{}'"'" | g++ -xc++ -o {} -)",
                    prog_text, test_prog_path);
  // spdlog::info("Cmd: {}", cmd);
  system(cmd.c_str());

  std::vector<const char *> args{test_prog_path.c_str(), nullptr};

  u_char val{};
  AnonymousPipe anon_pipe;

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child
    anon_pipe.CloseParentEnd();

    dup2(anon_pipe.GetChildEndFd(), 1);  // stdout -> pipe
    dup2(anon_pipe.GetChildEndFd(), 2);  // stderr -> pipe

    anon_pipe.ReadIntegerFromParent<u_char>(&val);
    anon_pipe.CloseChildEnd();  // This descriptor is no longer needed.

    std::vector<const char *> argv{test_prog_path.c_str(), nullptr};

    execvp(test_prog_path.c_str(), const_cast<char *const *>(argv.data()));
  } else {  // Parent
    anon_pipe.CloseChildEnd();

    struct bufferevent *ev_buf_event;
    ev_buf_event =
        bufferevent_socket_new(m_ev_base_, anon_pipe.GetParentEndFd(), 0);
    if (!ev_buf_event) {
      FAIL() << "Error constructing bufferevent!";
    }
    bufferevent_setcb(ev_buf_event, read_uv_stream, nullptr, nullptr, this);
    bufferevent_enable(ev_buf_event, EV_READ);
    bufferevent_disable(ev_buf_event, EV_WRITE);

    anon_pipe.WriteIntegerToChild<u_char>(val);

    // Persistent event. No need to reactivate it.
    this->m_ev_sigchld_ =
        evsignal_new(this->m_ev_base_, SIGCHLD, sigchld_handler_func, this);
    if (!this->m_ev_sigchld_) {
      FAIL() << "Could not create a signal event!";
    }

    if (event_add(this->m_ev_sigchld_, nullptr) < 0) {
      FAIL() << "Could not add a signal event!";
    }

    event_base_dispatch(m_ev_base_);
    bufferevent_free(ev_buf_event);
  }
  if (remove(test_prog_path.c_str()) != 0)
    spdlog::error("Error removing test_prog:", strerror(errno));
}
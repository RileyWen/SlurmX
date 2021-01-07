#include <fmt/format.h>
#include <unistd.h>
#include <sys/wait.h>

#include <csignal>

int main() {
  // Detach child process
  // sigignore(SIGCHLD);

  int pipe_fd[2];
  pipe(pipe_fd);

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child
    close(pipe_fd[0]);   // Close reading end

    dup2(pipe_fd[1], 1);  // stdout -> pipe
    dup2(pipe_fd[1], 2);  // stderr -> pipe

    close(pipe_fd[1]);  // This descriptor is no longer needed.

    // execve(........);

    fmt::print("This is a Test!");
  } else {  // Parent
    char buffer[1024];

    close(pipe_fd[1]); // Close writing end

    int nread;
    while ((nread = read(pipe_fd[0], buffer, sizeof(buffer))) != 0) {
      fmt::print("[Parent] Child sends: {:.{}s}\n", buffer, nread);
    }

    int status;
    wait(&status);
  }

  return 0;
}
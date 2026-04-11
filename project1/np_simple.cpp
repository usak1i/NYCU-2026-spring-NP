#include <cctype>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <array>
#include <vector>
#include <map>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

using namespace std;

struct Command {
    vector<string> argv;
    string output_file;
    int pipe_num = 0;
    bool is_err = false;
};

struct Pipeline {
    vector<Command> cmds;
};

// Per-client shell state (reset for each forked child)
int cmd_count = 0;
map<int, array<int,2>> pipe_table;

void sigchld_handler(int) {
    int saved_errno = errno;
    int status;
    while (waitpid(-1, &status, WNOHANG) > 0);
    errno = saved_errno;
}

int isNumberedPipe(string op, bool &is_err) {
    if (op.size() < 2 || (op[0] != '|' && op[0] != '!')) return -1;
    if (op[0] == '|') is_err = false;
    else if (op[0] == '!') is_err = true;

    int n = op.size();
    for (int i = 1; i < n; i++) {
        if (!isdigit(op[i])) return -1;
    }
    return stoi(op.substr(1));
}

vector<string> tokenize(const string &line) {
    vector<string> args;
    istringstream stream(line);
    string token;
    while (stream >> token) {
        args.push_back(token);
    }
    return args;
}

Pipeline parse(vector<string> &args) {
    Pipeline pipeline;
    Command cmd;

    for (size_t i = 0; i < args.size(); i++) {
        int n = isNumberedPipe(args[i], cmd.is_err);
        if (n > 0) {
            cmd.pipe_num = n;
            pipeline.cmds.push_back(cmd);
            cmd = Command{};
        } else if (args[i] == "|") {
            if (i + 1 >= args.size()) {
                cout << "No Pipe target" << endl;
                return pipeline;
            }
            pipeline.cmds.push_back(cmd);
            cmd = Command{};
        } else if (args[i] == ">") {
            if (i + 1 < args.size()) {
                cmd.output_file = args[i + 1];
                i++;
            } else {
                cout << "No Iuput file" << endl;
                return pipeline;
            }
            pipeline.cmds.push_back(cmd);
            cmd = Command{};
        } else {
            cmd.argv.push_back(args[i]);
        }
    }

    if (!cmd.argv.empty()) {
        pipeline.cmds.push_back(cmd);
    }
    return pipeline;
}

vector<char*> to_argv(vector<string> &args) {
    vector<char*> argv;
    for (auto &s : args) argv.push_back(&s[0]);
    argv.push_back(nullptr);
    return argv;
}

void redirection(const Command &cmd) {
    if (cmd.output_file.empty()) return;
    int fd = open(cmd.output_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open");
        exit(8);
    }
    dup2(fd, STDOUT_FILENO);
    close(fd);
}

void execute(Pipeline pipeline) {
    int n = pipeline.cmds.size();
    if (n == 0) return;

    int prev_read_fd = -1;
    bool new_segment = true;

    vector<pid_t> pids;
    for (int i = 0; i < n; i++) {
        Command &cmd = pipeline.cmds[i];

        int read_fd = -1;
        if (new_segment) {
            cmd_count++;
            new_segment = false;

            if (pipe_table.count(cmd_count)) {
                read_fd = pipe_table[cmd_count][0];
                close(pipe_table[cmd_count][1]);
                pipe_table.erase(cmd_count);
            }
        }

        if (prev_read_fd != -1) {
            if (read_fd != -1) close(read_fd);
            read_fd = prev_read_fd;
        } else if (read_fd == -1) {
            read_fd = STDIN_FILENO;
        }

        int write_fd = STDOUT_FILENO;
        int err_fd = STDERR_FILENO;
        int next_read_fd = -1;

        if (cmd.pipe_num > 0) {
            int target_cmd_id = cmd_count + cmd.pipe_num;
            if (pipe_table.find(target_cmd_id) == pipe_table.end()) {
                int fd[2];
                if (pipe(fd) < 0) {
                    perror("Failed to create numbered pipe");
                    exit(1);
                }
                pipe_table[target_cmd_id] = {fd[0], fd[1]};
            }
            new_segment = true;
            write_fd = pipe_table[target_cmd_id][1];
            if (cmd.is_err) err_fd = write_fd;
        } else if (i < n - 1) {
            int fd[2];
            if (pipe(fd) == -1) {
                perror("Failed to created pipe fd");
                exit(1);
            }
            write_fd = fd[1];
            next_read_fd = fd[0];
        }

        pid_t pid;
        while ((pid = fork()) < 0) {
            // System resources is full
            // Release any finished child process to release resource
            int status;
            waitpid(-1, &status, 0);
        }

        if (pid == 0) {
            // Child process
            // Child: restore default SIGCHLD so execvp'd programs behave normally
            signal(SIGCHLD, SIG_DFL);

            if (read_fd != STDIN_FILENO) {
                // STDIN pointing to previous pipe read end
                dup2(read_fd, STDIN_FILENO);
                close(read_fd);
            }
            if (write_fd != STDOUT_FILENO) {
                // STDOUT pointing to current pipe write end
                dup2(write_fd, STDOUT_FILENO);
            }
            if (err_fd != STDERR_FILENO) {
                dup2(err_fd, STDERR_FILENO);
            }
            if (write_fd != STDOUT_FILENO) close(write_fd);
            if (err_fd != STDERR_FILENO && err_fd != write_fd) close(err_fd);

            if (!cmd.output_file.empty()) {
                // Last command: check for redirection
                redirection(pipeline.cmds[i]);
            }

            // Cleaning stage, Close all unused fds
            if (next_read_fd != -1) close(next_read_fd);

            for (auto const& pair : pipe_table) {
                close(pair.second[0]);
                close(pair.second[1]);
            }

            vector<char*> argv = to_argv(pipeline.cmds[i].argv);
            execvp(argv[0], argv.data());

            fprintf(stderr, "Unknown command: [%s].\n", argv[0]);
            exit(3);
        }

        if (cmd.pipe_num == 0) pids.push_back(pid);

        if (read_fd != STDIN_FILENO) close(read_fd);

        if (i < n - 1 && cmd.pipe_num == 0) {
            close(write_fd);
            prev_read_fd = next_read_fd;
        } else {
            prev_read_fd = -1;
        }
    }

    for (pid_t pid : pids) {
        int status;
        waitpid(pid, &status, 0);
    }
}

// Run the shell loop on the connected client socket.
// stdin/stdout/stderr have already been dup2'd to the socket.
void run_shell() {
    setenv("PATH", "bin:.", 1);
    cmd_count = 0;
    pipe_table.clear();

    while (true) {
        int status;
        while (waitpid(-1, &status, WNOHANG) > 0);

        cout << "% ";
        cout.flush();

        string line;
        if (!getline(cin, line)) break;

        // Strip trailing \r from telnet line endings
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
            line.pop_back();
        }

        if (line.empty()) continue;
        if (line == "exit") break;

        vector<string> tokens = tokenize(line);
        if (tokens.empty()) continue;

        Pipeline pipeline = parse(tokens);

        if (tokens[0] == "exit") {
            break;
        } else if (tokens[0] == "setenv" && tokens.size() >= 3) {
            setenv(tokens[1].c_str(), tokens[2].c_str(), 1);
            cmd_count++;
            if (pipe_table.count(cmd_count)) {
                close(pipe_table[cmd_count][0]);
                close(pipe_table[cmd_count][1]);
                pipe_table.erase(cmd_count);
            }
            continue;
        } else if (tokens[0] == "printenv" && tokens.size() >= 2) {
            const char* val = getenv(tokens[1].c_str());
            if (val != nullptr) cout << val << endl;
            cmd_count++;
            if (pipe_table.count(cmd_count)) {
                close(pipe_table[cmd_count][0]);
                close(pipe_table[cmd_count][1]);
                pipe_table.erase(cmd_count);
            }
            continue;
        }

        execute(pipeline);
    }

    // Clean up any numbered pipes still open in this client's session
    for (auto const& pair : pipe_table) {
        close(pair.second[0]);
        close(pair.second[1]);
    }
    pipe_table.clear();
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        return 1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    // Call socket::bind instead of std::bind
    if (::bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(listen_fd, 16) < 0) {
        perror("listen");
        return 1;
    }

    // Reap zombie children automatically
    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, nullptr);

    while (true) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client_fd = accept(listen_fd, (struct sockaddr*)&cli_addr, &cli_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            close(client_fd);
            continue;
        }

        if (pid == 0) {
            // Child: this client's private shell session
            close(listen_fd);

            dup2(client_fd, STDIN_FILENO);
            dup2(client_fd, STDOUT_FILENO);
            dup2(client_fd, STDERR_FILENO);
            close(client_fd);

            // Make C++ streams unbuffered-ish so telnet sees output promptly
            ios_base::sync_with_stdio(true);
            cin.tie(nullptr);
            setvbuf(stdout, nullptr, _IONBF, 0);
            setvbuf(stderr, nullptr, _IONBF, 0);

            run_shell();
            exit(0);
        }

        close(client_fd);
    }

    return 0;
}

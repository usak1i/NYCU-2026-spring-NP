#include <cctype>
#include <cstdio>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <string>
#include <cstdio>
#include <sys/resource.h>
#include <sys/wait.h>
#include <array>
#include <vector>
#include <map>
#include <unistd.h>
#include <stdlib.h>

using namespace std;

struct Command {
    vector<string> argv;
    string output_file; // Store the filename of the output file when encountered ">"
    int pipe_num = 0;
    bool is_err = false;
};

struct Pipeline {
    vector<Command> cmds;
};

// Counter for numberedpipe
int cmd_count = 0;

// Records numbered pipe readfd and writefd
// target_cmd = cmd_count + N (|N)
// <key: target_cmd, <read_fd, write_fd>>
map<int, array<int,2>> pipe_table;


int state;

void handler(int signo) {
    while (waitpid(-1, &state, WNOHANG) > 0);
}

int isNumberedPipe(string op, bool &is_err) {
    if (op.size() < 2 || (op[0] != '|' && op[0] != '!')) return -1;
    if (op[0] == '|') is_err = false;
    else if (op[0] == '!') is_err = true;

    int n = op.size();

    for (int i = 1; i < n; i++) {
        if (!isdigit(op[i]))
            return -1;
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

// tokens to vector<Command>
Pipeline parse(vector<string> &args) {
    Pipeline pipeline;
    Command cmd;

    for (int i = 0; i < args.size(); i++) {
        int n = isNumberedPipe(args[i], cmd.is_err);
        if (n > 0) {
            // Numbered Pipe
            cmd.pipe_num = n;
            pipeline.cmds.push_back(cmd);
            cmd = Command{};
        } else if (args[i] == "|") {
            if (i + 1 >= args.size()) {
                cout << "No Pipe target" << endl;
                exit(7);
            }
            pipeline.cmds.push_back(cmd);
            cmd = Command{};
        } else if (args[i] == ">") {
            if (i + 1 < args.size()) {
                // Expect the next arg should be the output file
                cmd.output_file = args[i + 1];
                // skip one index (filename)
                i++;
            } else {
                cout << "No Iuput file" << endl;
                exit(6);
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
};

// Transform args to execvp() argv
vector<char*> to_argv(vector<string> &args) {
    vector<char*> argv;
    for (auto &s : args) {
        argv.push_back(&s[0]);
    }
    argv.push_back(nullptr);
    return argv;
};

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

    int prev_read_fd = -1; // initial STDIN
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
            if (read_fd != -1) {
                close(read_fd);
            }
            read_fd = prev_read_fd;
        } else if (read_fd == -1) {
            read_fd = STDIN_FILENO;
        }

        int write_fd = STDOUT_FILENO;
        int err_fd = STDERR_FILENO;
        int next_read_fd = -1;

        if (cmd.pipe_num > 0) {
            // Numberedpipe
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
            if (cmd.is_err) {
                err_fd = write_fd;
            }
        } else if (i < n - 1) {
            // Ordinary pipe
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

            if (write_fd != STDOUT_FILENO) {
                close(write_fd);
            }

            if (err_fd != STDERR_FILENO && err_fd != write_fd) {
                close(err_fd);
            }

            if (!cmd.output_file.empty()) {
                // Last command: check for redirection
                redirection(pipeline.cmds[i]);
            }

            // Cleaning stage, Close all unused fds
            if (next_read_fd != -1) {
                close(next_read_fd);
            }

            for (auto const& pair : pipe_table) {
                close(pair.second[0]);
                close(pair.second[1]);
            }

            vector<char*> argv = to_argv(pipeline.cmds[i].argv);
            execvp(argv[0], argv.data());

            fprintf(stderr, "Unknown command: [%s].\n", argv[0]);
            exit(3);
        }

        // Parent process
        if (cmd.pipe_num == 0) {
            pids.push_back(pid);
        }

        if (read_fd != STDIN_FILENO) {
            close(read_fd);
        }

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


int main(int argc, char *argv[]) {
    // signal(SIGCHLD, handler);
    setenv("PATH", "bin:.", 1);

    while (true) {
        int status;
        while (waitpid(-1, &status, WNOHANG) > 0);

        cout << "% ";
        cout.flush();

        string line;
        if (!getline(cin, line)) {
            cout << "\n";
            break;
        }

        if (line.empty()) continue;
        if (line == "exit") break;

        vector<string> tokens = tokenize(line);
        if (tokens.empty()) continue;

        Pipeline pipeline = parse(tokens);

        if (tokens[0] == "exit") {
            exit(0);
            continue;
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
            if (val != nullptr) {
                cout << val << endl;
            }
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
    return 0;
}

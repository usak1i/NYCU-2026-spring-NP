// np_single_proc.cpp
//
// Single-process concurrent "Remote Working Ground" shell server.
// All clients are multiplexed through a single select() loop; commands still
// fork()/execvp() so user programs run as separate processes, but the server
// itself never forks per client.
//
// Build: g++ -std=c++11 -Wall -o np_single_proc np_single_proc.cpp

#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <map>
#include <vector>

using std::array;
using std::map;
using std::string;
using std::vector;

// ------------------------------ Configuration --------------------------------

static const int MAX_USERS      = 30;
static const int MAX_NICK_LEN   = 20;
static const char *USER_PIPE_DIR = "user_pipe";

// ------------------------------ Data model -----------------------------------

struct Command {
    vector<string> argv;
    string output_file;    // "> file"
    int pipe_num = 0;      // |N / !N : pipe to N-th future command
    bool is_err = false;   // true for !N
    int user_pipe_to = 0;  // >n : write to user n
    int user_pipe_from = 0;// <n : read from user n
};

struct Pipeline {
    vector<Command> cmds;
};

struct UserSlot {
    bool active = false;
    int  fd = -1;
    int  id = 0;
    char nickname[MAX_NICK_LEN + 1] = {0};
    char ip[INET_ADDRSTRLEN] = {0};
    int  port = 0;

    // Per-client shell state
    int cmd_count = 0;
    map<int, array<int, 2>> pipe_table;   // target cmd_count -> {read_fd, write_fd}

    // Per-client environment (PATH, etc.) applied before each execute()
    map<string, string> env;

    // Line-oriented input buffer (telnet lines may arrive in fragments)
    string inbuf;

};

static UserSlot g_users[MAX_USERS];
static int      g_listen_fd = -1;

// ------------------------------ Utilities ------------------------------------

static void sigchld_handler(int) {
    int saved = errno;
    while (waitpid(-1, nullptr, WNOHANG) > 0) {}
    errno = saved;
}

// Find a user slot by ID (1..MAX_USERS).  Returns nullptr if not active.
static UserSlot *find_user_by_id(int id) {
    if (id < 1 || id > MAX_USERS) return nullptr;
    UserSlot &u = g_users[id - 1];
    return u.active ? &u : nullptr;
}

// Find an active user by nickname.
static UserSlot *find_user_by_name(const string &n) {
    for (int i = 0; i < MAX_USERS; i++) {
        if (g_users[i].active && n == g_users[i].nickname) return &g_users[i];
    }
    return nullptr;
}

// Robust write to client fd.  SIGPIPE is ignored globally so EPIPE surfaces.
static void write_to_fd(int fd, const string &s) {
    if (fd < 0) return;
    const char *p = s.data();
    size_t left = s.size();
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            return; // client likely gone; main loop will clean up on next read
        }
        p += n;
        left -= (size_t)n;
    }
}

static void send_to_user(UserSlot &u, const string &s) {
    write_to_fd(u.fd, s);
}

static void broadcast(const string &s) {
    for (int i = 0; i < MAX_USERS; i++) {
        if (g_users[i].active) write_to_fd(g_users[i].fd, s);
    }
}

static void send_prompt(UserSlot &u) {
    send_to_user(u, "% ");
}

static void send_welcome(UserSlot &u) {
    static const char *banner =
        "****************************************\n"
        "** Welcome to the information server. **\n"
        "****************************************\n";
    send_to_user(u, banner);
}

// Format "user_pipe/<sender>_<receiver>"
static string user_pipe_path(int sender_id, int receiver_id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/%d_%d", USER_PIPE_DIR, sender_id, receiver_id);
    return string(buf);
}

static bool file_exists(const string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

// Remove every file in user_pipe/ that involves this user as sender or receiver.
static void cleanup_user_pipes(int id) {
    DIR *d = opendir(USER_PIPE_DIR);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != nullptr) {
        if (de->d_name[0] == '.') continue;
        // Parse "<sender>_<receiver>"
        int s = 0, r = 0;
        if (sscanf(de->d_name, "%d_%d", &s, &r) != 2) continue;
        if (s == id || r == id) {
            string p = string(USER_PIPE_DIR) + "/" + de->d_name;
            unlink(p.c_str());
        }
    }
    closedir(d);
}

// Check nickname rule: 1..20, alphanumeric only.
static bool valid_nickname(const string &n) {
    if (n.empty() || n.size() > MAX_NICK_LEN) return false;
    for (char c : n) if (!isalnum((unsigned char)c)) return false;
    return true;
}

// --------------------------- Tokenize / Parse --------------------------------

static int isNumberedPipe(const string &op, bool &is_err) {
    if (op.size() < 2 || (op[0] != '|' && op[0] != '!')) return -1;
    is_err = (op[0] == '!');
    for (size_t i = 1; i < op.size(); i++) {
        if (!isdigit((unsigned char)op[i])) return -1;
    }
    return atoi(op.c_str() + 1);
}

// Returns >0 for ">n"  (user pipe target)
// Returns -1 for ">file" placeholder (caller must consume next token)
// Returns 0 if token isn't a '>' form.
static int parseGtUserPipe(const string &tok) {
    if (tok.empty() || tok[0] != '>') return 0;
    if (tok.size() == 1) return -1; // ">" alone -> "> file"
    for (size_t i = 1; i < tok.size(); i++) {
        if (!isdigit((unsigned char)tok[i])) return 0;
    }
    return atoi(tok.c_str() + 1);
}

// Returns >0 for "<n"  (user pipe source), 0 otherwise.
static int parseLtUserPipe(const string &tok) {
    if (tok.size() < 2 || tok[0] != '<') return 0;
    for (size_t i = 1; i < tok.size(); i++) {
        if (!isdigit((unsigned char)tok[i])) return 0;
    }
    return atoi(tok.c_str() + 1);
}

static vector<string> tokenize(const string &line) {
    vector<string> args;
    std::istringstream ss(line);
    string t;
    while (ss >> t) args.push_back(t);
    return args;
}

static Pipeline parse(const vector<string> &args) {
    Pipeline p;
    Command cmd;

    for (size_t i = 0; i < args.size(); i++) {
        const string &a = args[i];
        bool is_err = false;
        int np = isNumberedPipe(a, is_err);
        int up_to = parseGtUserPipe(a);
        int up_from = parseLtUserPipe(a);

        if (np > 0) {
            cmd.pipe_num = np;
            cmd.is_err = is_err;
            p.cmds.push_back(cmd);
            cmd = Command{};
        } else if (a == "|") {
            p.cmds.push_back(cmd);
            cmd = Command{};
        } else if (up_to == -1) {   // "> file"
            if (i + 1 < args.size()) {
                cmd.output_file = args[++i];
            }
        } else if (up_to > 0) {     // ">n"
            cmd.user_pipe_to = up_to;
        } else if (up_from > 0) {   // "<n"
            cmd.user_pipe_from = up_from;
        } else {
            cmd.argv.push_back(a);
        }
    }

    if (!cmd.argv.empty() || cmd.pipe_num > 0 ||
        cmd.user_pipe_to > 0 || cmd.user_pipe_from > 0 ||
        !cmd.output_file.empty()) {
        p.cmds.push_back(cmd);
    }
    return p;
}

static vector<char*> to_argv(vector<string> &args) {
    vector<char*> v;
    v.reserve(args.size() + 1);
    for (auto &s : args) v.push_back(&s[0]);
    v.push_back(nullptr);
    return v;
}

// Apply per-client environment (PATH etc.) before fork/execvp.
static void apply_env(UserSlot &u) {
    for (auto &kv : u.env) {
        setenv(kv.first.c_str(), kv.second.c_str(), 1);
    }
}

// ------------------------------ Execution ------------------------------------

// User-pipe storage uses regular files so data persists between the sender's
// write and the receiver's read regardless of timing.
static int open_user_pipe_write(const string &path) {
    return open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
}

static int open_user_pipe_read(const string &path) {
    return open(path.c_str(), O_RDONLY);
}

// Close all other clients' fds and their pipe_table fds in a forked child.
// Leaving other users' pipe write-ends open would prevent their readers
// from ever seeing EOF.
static void close_other_client_fds(int my_fd) {
    for (int i = 0; i < MAX_USERS; i++) {
        UserSlot &s = g_users[i];
        if (!s.active) continue;
        if (s.fd != my_fd) close(s.fd);
        // Close numbered-pipe fds belonging to other users.
        // (Current user's pipe_table fds are handled inside execute().)
        if (s.fd != my_fd) {
            for (auto &kv : s.pipe_table) {
                close(kv.second[0]);
                close(kv.second[1]);
            }
        }
    }
    if (g_listen_fd >= 0) close(g_listen_fd);
}

// Forward declaration — defined after execute().
static void consume_cmd_slot(UserSlot &u);

// Execute a parsed pipeline on behalf of user `u`.
// `whole_line` is the original command line, used for user-pipe broadcasts.
static void execute(UserSlot &u, Pipeline pipeline, const string &whole_line) {
    int n = (int)pipeline.cmds.size();
    if (n == 0) return;

    int up_from = 0, up_to = 0;
    for (auto &c : pipeline.cmds) {
        if (c.user_pipe_from && !up_from) up_from = c.user_pipe_from;
        if (c.user_pipe_to   && !up_to)   up_to   = c.user_pipe_to;
        c.user_pipe_from = 0;
        c.user_pipe_to   = 0;
    }

    // ----- Validate user-pipe errors in the order: "<n" first, then ">n".
    int up_in_fd = -1;   // read end for the first command's stdin (-1 = /dev/null)
    int up_out_fd = -1;  // write end for the last command's stdout (-1 = socket)
    bool up_from_ok = false;
    bool up_to_ok   = false;
    UserSlot *sender_u = nullptr;
    UserSlot *recv_u   = nullptr;

    if (up_from > 0) {
        sender_u = find_user_by_id(up_from);
        if (!sender_u) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "*** Error: user #%d does not exist yet. ***\n", up_from);
            send_to_user(u, buf);
        } else {
            string p = user_pipe_path(up_from, u.id);
            if (!file_exists(p)) {
                char buf[128];
                snprintf(buf, sizeof(buf),
                         "*** Error: the pipe #%d->#%d does not exist yet. ***\n",
                         up_from, u.id);
                send_to_user(u, buf);
            } else {
                up_in_fd = open_user_pipe_read(p);
                if (up_in_fd >= 0) {
                    up_from_ok = true;
                    // One-shot: unlink now so the inode disappears once our fd
                    // closes, freeing the name for a future pipe.
                    unlink(p.c_str());
                }
            }
        }
    }

    if (up_to > 0) {
        recv_u = find_user_by_id(up_to);
        if (!recv_u) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "*** Error: user #%d does not exist yet. ***\n", up_to);
            send_to_user(u, buf);
        } else {
            string p = user_pipe_path(u.id, up_to);
            if (file_exists(p)) {
                char buf[128];
                snprintf(buf, sizeof(buf),
                         "*** Error: the pipe #%d->#%d already exists. ***\n",
                         u.id, up_to);
                send_to_user(u, buf);
            } else {
                up_to_ok = true;
            }
        }
    }

    // Broadcasts: receive message first, then send message (per spec §3.5 item 6).
    if (up_from_ok) {
        char buf[8192];
        snprintf(buf, sizeof(buf),
                 "*** %s (#%d) just received from %s (#%d) by '%s' ***\n",
                 u.nickname, u.id, sender_u->nickname, sender_u->id,
                 whole_line.c_str());
        broadcast(buf);
    }
    if (up_to_ok) {
        string p = user_pipe_path(u.id, up_to);
        up_out_fd = open_user_pipe_write(p);
        if (up_out_fd < 0) {
            unlink(p.c_str());
            up_to_ok = false;
        } else {
            char buf[8192];
            snprintf(buf, sizeof(buf),
                     "*** %s (#%d) just piped '%s' to %s (#%d) ***\n",
                     u.nickname, u.id, whole_line.c_str(),
                     recv_u->nickname, recv_u->id);
            broadcast(buf);
        }
    }

    // Always fall through to execute — do NOT return early on pipe errors.

    int prev_read_fd = -1;
    bool new_segment = true;
    vector<pid_t> pids;

    for (int i = 0; i < n; i++) {
        Command &cmd = pipeline.cmds[i];

        int read_fd = -1;
        if (new_segment) {
            u.cmd_count++;
            new_segment = false;

            if (u.pipe_table.count(u.cmd_count)) {
                read_fd = u.pipe_table[u.cmd_count][0];
                close(u.pipe_table[u.cmd_count][1]);
                u.pipe_table.erase(u.cmd_count);
            }
        }

        // First command may also pull stdin from a user pipe.
        if (i == 0 && up_in_fd >= 0) {
            if (read_fd != -1) close(read_fd);
            read_fd = up_in_fd;
            up_in_fd = -1;
        }

        if (prev_read_fd != -1) {
            if (read_fd != -1) close(read_fd);
            read_fd = prev_read_fd;
        } else if (read_fd == -1) {
            read_fd = STDIN_FILENO;
        }

        int write_fd = STDOUT_FILENO;
        int err_fd   = STDERR_FILENO;
        int next_read_fd = -1;

        if (cmd.pipe_num > 0) {
            int target = u.cmd_count + cmd.pipe_num;
            if (u.pipe_table.find(target) == u.pipe_table.end()) {
                int fd[2];
                if (pipe(fd) < 0) {
                    perror("pipe");
                    if (prev_read_fd >= 0) close(prev_read_fd);
                    if (up_in_fd  >= 0) close(up_in_fd);
                    if (up_out_fd >= 0) close(up_out_fd);
                    for (pid_t p : pids) waitpid(p, nullptr, 0);
                    return;
                }
                u.pipe_table[target] = {fd[0], fd[1]};
            }
            new_segment = true;
            write_fd = u.pipe_table[target][1];
            if (cmd.is_err) err_fd = write_fd;
        } else if (i < n - 1) {
            int fd[2];
            if (pipe(fd) < 0) {
                perror("pipe");
                if (prev_read_fd >= 0) close(prev_read_fd);
                if (up_in_fd  >= 0) close(up_in_fd);
                if (up_out_fd >= 0) close(up_out_fd);
                for (pid_t p : pids) waitpid(p, nullptr, 0);
                return;
            }
            write_fd = fd[1];
            next_read_fd = fd[0];
        } else {
            // Last command: if user-pipe-to was set, redirect stdout to file.
            if (up_out_fd >= 0) {
                write_fd = up_out_fd;
            }
        }

        pid_t pid;
        while ((pid = fork()) < 0) {
            int status;
            waitpid(-1, &status, 0);
        }

        if (pid == 0) {
            // -------------------- Child --------------------
            signal(SIGCHLD, SIG_DFL);
            signal(SIGPIPE, SIG_DFL);

            // Apply this client's environment (PATH etc.) in the child only,
            // so per-user env never leaks into the parent's global state.
            apply_env(u);

            // Hide other clients' fds and the listen fd from exec'd program.
            close_other_client_fds(u.fd);
            if (read_fd != STDIN_FILENO) {
                dup2(read_fd, STDIN_FILENO);
            }
            if (write_fd != STDOUT_FILENO) {
                dup2(write_fd, STDOUT_FILENO);
            }
            if (err_fd != STDERR_FILENO) {
                dup2(err_fd, STDERR_FILENO);
            }

            if (read_fd != STDIN_FILENO) close(read_fd);
            if (write_fd != STDOUT_FILENO) close(write_fd);
            if (err_fd != STDERR_FILENO && err_fd != write_fd) close(err_fd);
            if (next_read_fd != -1) close(next_read_fd);

            // Close all numbered-pipe fds owned by this client (except those
            // already dup'd).  Parent will keep its copy.
            for (auto const &kv : u.pipe_table) {
                close(kv.second[0]);
                close(kv.second[1]);
            }

            // If we also have an unused up_out_fd for a non-last command
            // (shouldn't happen — we only set up_out_fd on last), close it.
            if (up_out_fd >= 0 && write_fd != up_out_fd) close(up_out_fd);

            if (write_fd == STDOUT_FILENO) {
                if (up_to > 0) {
                    // >n was requested but failed; discard stdout
                    int devnull = open("/dev/null", O_WRONLY);
                    if (devnull >= 0) { dup2(devnull, STDOUT_FILENO); close(devnull); }
                } else {
                    dup2(u.fd, STDOUT_FILENO);
                }
            }
            if (err_fd == STDERR_FILENO) {
                dup2(u.fd, STDERR_FILENO);
            }
            if (read_fd == STDIN_FILENO) {
                // No stdin from pipeline: give /dev/null so programs like
                // cat don't stall reading from the socket.
                int devnull = open("/dev/null", O_RDONLY);
                if (devnull >= 0) {
                    dup2(devnull, STDIN_FILENO);
                    close(devnull);
                }
            }
            close(u.fd);

            if (!cmd.output_file.empty()) {
                int f = open(cmd.output_file.c_str(),
                             O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (f < 0) {
                    perror("open");
                    _exit(1);
                }
                dup2(f, STDOUT_FILENO);
                close(f);
            }

            if (cmd.argv.empty()) _exit(0);

            // On some Linux kernels a dup2'd file descriptor must be touched
            // by a syscall before exec or the exec'd program sees EBADF on it.
            if (read_fd != STDIN_FILENO) {
                struct stat _st;
                (void)fstat(STDIN_FILENO, &_st);
            }

            vector<char*> argv = to_argv(cmd.argv);
            execvp(argv[0], argv.data());
            fprintf(stderr, "Unknown command: [%s].\n", argv[0]);
            _exit(1);
        }

        // -------------------- Parent --------------------
        if (cmd.pipe_num == 0) pids.push_back(pid);

        if (read_fd != STDIN_FILENO) close(read_fd);

        if (i < n - 1 && cmd.pipe_num == 0) {
            close(write_fd);
            prev_read_fd = next_read_fd;
        } else {
            if (up_out_fd >= 0 && write_fd == up_out_fd) {
                // Regular file: close the write fd now — data is on disk.
                close(up_out_fd);
                up_out_fd = -1;
            }
            prev_read_fd = -1;
        }
    }

    // Safety: close stragglers.
    if (up_in_fd  >= 0) close(up_in_fd);
    if (up_out_fd >= 0) { close(up_out_fd); up_out_fd = -1; }

    for (pid_t pid : pids) {
        int status;
        waitpid(pid, &status, 0);
    }

}

// --------------------------- Built-in handlers -------------------------------

// Advance cmd_count and discard any pending numbered-pipe fds for this slot.
static void consume_cmd_slot(UserSlot &u) {
    u.cmd_count++;
    auto it = u.pipe_table.find(u.cmd_count);
    if (it != u.pipe_table.end()) {
        close(it->second[0]);
        close(it->second[1]);
        u.pipe_table.erase(it);
    }
}

static void do_who(UserSlot &me) {
    string out = "<ID>\t<fd>\t<nickname>\t<IP:port>\t<indicate me>\n";
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_users[i].active) continue;
        UserSlot &u = g_users[i];
        char row[256];
        snprintf(row, sizeof(row), "%d\t%d\t%s\t%s:%d%s\n",
                 u.id, u.fd, u.nickname, u.ip, u.port,
                 (&u == &me) ? "\t<-me" : "");
        out += row;
    }
    send_to_user(me, out);
    consume_cmd_slot(me);
}

static void do_name(UserSlot &me, const string &nn) {
    if (!valid_nickname(nn)) {
        // Silently ignore invalid nicks (spec doesn't define an error msg).
        consume_cmd_slot(me);
        return;
    }
    if (find_user_by_name(nn)) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "*** User '%s' already exists. ***\n", nn.c_str());
        send_to_user(me, buf);
        consume_cmd_slot(me);
        return;
    }
    snprintf(me.nickname, sizeof(me.nickname), "%s", nn.c_str());
    char buf[256];
    snprintf(buf, sizeof(buf),
             "*** User from %s:%d is named '%s'. ***\n",
             me.ip, me.port, me.nickname);
    broadcast(buf);
    consume_cmd_slot(me);
}

static void do_yell(UserSlot &me, const string &msg) {
    char buf[2048];
    snprintf(buf, sizeof(buf),
             "*** %s yelled ***: %s\n", me.nickname, msg.c_str());
    broadcast(buf);
    consume_cmd_slot(me);
}

static void do_tell(UserSlot &me, int target_id, const string &msg) {
    UserSlot *t = find_user_by_id(target_id);
    if (!t) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "*** Error: user #%d does not exist yet. ***\n", target_id);
        send_to_user(me, buf);
        consume_cmd_slot(me);
        return;
    }
    char buf[2048];
    snprintf(buf, sizeof(buf),
             "*** %s told you ***: %s\n", me.nickname, msg.c_str());
    send_to_user(*t, buf);
    consume_cmd_slot(me);
}

// --------------------------- Line processing ---------------------------------

// Process a full line from user `u`.
// Returns true if the user should be disconnected (e.g. after `exit`).
static bool process_line(UserSlot &u, const string &raw) {
    // Strip trailing CR/LF
    string line = raw;
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
        line.pop_back();
    }
    if (line.empty()) return false;

    vector<string> tokens = tokenize(line);
    if (tokens.empty()) return false;

    const string &cmd0 = tokens[0];

    if (cmd0 == "exit") {
        return true;
    }

    if (cmd0 == "setenv" && tokens.size() >= 3) {
        u.env[tokens[1]] = tokens[2];
        consume_cmd_slot(u);
        return false;
    }
    if (cmd0 == "printenv" && tokens.size() >= 2) {
        auto it = u.env.find(tokens[1]);
        if (it != u.env.end()) send_to_user(u, it->second + "\n");
        consume_cmd_slot(u);
        return false;
    }
    if (cmd0 == "who") {
        do_who(u);
        return false;
    }
    if (cmd0 == "name") {
        string nn = (tokens.size() >= 2) ? tokens[1] : string();
        do_name(u, nn);
        return false;
    }
    if (cmd0 == "yell") {
        // Message is everything after "yell "
        size_t pos = line.find_first_not_of(" \t");
        pos = line.find_first_of(" \t", pos);
        string msg;
        if (pos != string::npos) {
            size_t start = line.find_first_not_of(" \t", pos);
            if (start != string::npos) msg = line.substr(start);
        }
        do_yell(u, msg);
        return false;
    }
    if (cmd0 == "tell" && tokens.size() >= 2) {
        int target = atoi(tokens[1].c_str());
        // Find "tell <id>" then grab the rest as message
        size_t p = line.find_first_not_of(" \t");
        p = line.find_first_of(" \t", p);
        p = line.find_first_not_of(" \t", p);
        p = line.find_first_of(" \t", p);
        string msg;
        if (p != string::npos) {
            size_t s = line.find_first_not_of(" \t", p);
            if (s != string::npos) msg = line.substr(s);
        }
        do_tell(u, target, msg);
        return false;
    }

    Pipeline p = parse(tokens);
    execute(u, p, line);
    return false;
}

// --------------------------- Client lifecycle --------------------------------

static int alloc_slot() {
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_users[i].active) return i;
    }
    return -1;
}

static void login_client(int fd, const sockaddr_in &addr) {
    int idx = alloc_slot();
    if (idx < 0) {
        const char *msg = "Server full.\n";
        write(fd, msg, strlen(msg));
        close(fd);
        return;
    }
    UserSlot &u = g_users[idx];
    u.active = true;
    u.fd = fd;
    u.id = idx + 1;
    strcpy(u.nickname, "(no name)");
    inet_ntop(AF_INET, &addr.sin_addr, u.ip, sizeof(u.ip));
    u.port = ntohs(addr.sin_port);
    u.cmd_count = 0;
    u.pipe_table.clear();
    u.env.clear();
    u.env["PATH"] = "bin:.";
    u.inbuf.clear();

    send_welcome(u);

    char buf[256];
    snprintf(buf, sizeof(buf),
             "*** User '(no name)' entered from %s:%d. ***\n", u.ip, u.port);
    broadcast(buf);

    send_prompt(u);
}

static void logout_client(UserSlot &u) {
    if (!u.active) return;

    // Broadcast leave to everyone else (including self doesn't matter since
    // we close right after; spec says broadcast to remaining users).
    char buf[128];
    snprintf(buf, sizeof(buf), "*** User '%s' left. ***\n", u.nickname);

    int leaving_fd = u.fd;
    int leaving_id = u.id;

    // Close this user's numbered-pipe fds.
    for (auto &kv : u.pipe_table) {
        close(kv.second[0]);
        close(kv.second[1]);
    }
    u.pipe_table.clear();

    // Deactivate BEFORE broadcasting so the leaving user doesn't receive it.
    u.active = false;
    u.fd = -1;
    u.id = 0;
    u.nickname[0] = '\0';
    u.ip[0] = '\0';
    u.port = 0;
    u.cmd_count = 0;
    u.env.clear();
    u.inbuf.clear();

    broadcast(buf);

    // Clean up FIFOs involving this user.
    cleanup_user_pipes(leaving_id);

    close(leaving_fd);
}

// Read data from a client fd, append to its line buffer, and process every
// complete line.  Returns true if the client should be disconnected.
static bool handle_client_readable(UserSlot &u) {
    char tmp[4096];
    ssize_t n = read(u.fd, tmp, sizeof(tmp));
    if (n == 0) return true;
    if (n < 0) {
        if (errno == EINTR || errno == EAGAIN) return false;
        return true;
    }
    u.inbuf.append(tmp, (size_t)n);

    bool disconnect = false;
    size_t pos;
    while ((pos = u.inbuf.find('\n')) != string::npos) {
        string line = u.inbuf.substr(0, pos);
        u.inbuf.erase(0, pos + 1);
        if (process_line(u, line)) {
            disconnect = true;
            break;
        }
        send_prompt(u);
    }
    return disconnect;
}

// ------------------------------- main ----------------------------------------

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        return 1;
    }
    int port = atoi(argv[1]);

    // Ensure user_pipe/ exists.
    if (mkdir(USER_PIPE_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir user_pipe");
        return 1;
    }
    // Also wipe stale FIFOs from a previous run.
    {
        DIR *d = opendir(USER_PIPE_DIR);
        if (d) {
            struct dirent *de;
            while ((de = readdir(d)) != nullptr) {
                if (de->d_name[0] == '.') continue;
                string p = string(USER_PIPE_DIR) + "/" + de->d_name;
                unlink(p.c_str());
            }
            closedir(d);
        }
    }

    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa = {};
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, nullptr);

    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    if (setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0) {
        perror("setsockopt"); return 1;
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (::bind(g_listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(g_listen_fd, 16) < 0) {
        perror("listen"); return 1;
    }

    for (;;) {
        fd_set rset;
        FD_ZERO(&rset);
        FD_SET(g_listen_fd, &rset);
        int maxfd = g_listen_fd;
        for (int i = 0; i < MAX_USERS; i++) {
            if (g_users[i].active) {
                FD_SET(g_users[i].fd, &rset);
                if (g_users[i].fd > maxfd) maxfd = g_users[i].fd;
            }
        }

        int r = select(maxfd + 1, &rset, nullptr, nullptr, nullptr);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        if (FD_ISSET(g_listen_fd, &rset)) {
            sockaddr_in cli;
            socklen_t clilen = sizeof(cli);
            int cfd = accept(g_listen_fd, (sockaddr*)&cli, &clilen);
            if (cfd >= 0) {
                login_client(cfd, cli);
            } else if (errno != EINTR) {
                perror("accept");
            }
        }

        for (int i = 0; i < MAX_USERS; i++) {
            UserSlot &u = g_users[i];
            if (!u.active) continue;
            if (!FD_ISSET(u.fd, &rset)) continue;

            if (handle_client_readable(u)) {
                logout_client(u);
            }
        }
    }

    close(g_listen_fd);
    return 0;
}

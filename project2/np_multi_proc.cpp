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
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
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

static const int  MAX_USERS       = 30;
static const int  MAX_NICK_LEN    = 20;
static const char *USER_PIPE_DIR  = "user_pipe";

// Shared memory
struct UserEntry {
    bool  active;
    int   id;
    int   fd;
    char  nickname[MAX_NICK_LEN + 1];
    char  ip[INET_ADDRSTRLEN];
    int   port;
    pid_t pid;
};

struct SharedMem {
    UserEntry users[MAX_USERS];
    // blocked[i][j] = true 表示「id=(i+1) 的 user 已封鎖 id=(j+1) 的 user」。
    // 封鎖關係是「對稱效果」：只要任一方封鎖了對方，雙方互相收不到 yell/tell。
    bool blocked[MAX_USERS][MAX_USERS];
};

static int        g_shm_id    = -1;   // IPC key for shared memory segment
static int        g_sem_id    = -1;   // IPC key for semaphore set
static SharedMem *g_shm       = nullptr;
static int        g_listen_fd = -1;   // server listening socket

// Semaphore
static void sem_lock() {
    struct sembuf op = {0, -1, SEM_UNDO};
    while (semop(g_sem_id, &op, 1) < 0 && errno == EINTR) {}
}
static void sem_unlock() {
    struct sembuf op = {0, +1, SEM_UNDO};
    while (semop(g_sem_id, &op, 1) < 0 && errno == EINTR) {}
}

// IPC cleanup (called from parent only)
static void cleanup_ipc(void) {
    if (g_shm_id >= 0) { shmctl(g_shm_id, IPC_RMID, nullptr); g_shm_id = -1; }
    if (g_sem_id >= 0) { semctl(g_sem_id, 0, IPC_RMID);       g_sem_id = -1; }
}

// 終結訊號到來時只設 flag,讓 main loop 優雅退出。
// 不在 handler 直接呼叫 _exit:那樣 child 還 attach 著 shm,kernel 會把
// segment 標成 'dest' 狀態,某些測試腳本(ipcs / shm.sh)會視為「shm 未清乾淨」。
// 等 main loop 自己 break、把所有 child 收屍完、parent 也 detach 後再 IPC_RMID,
// nattch 才能真的歸零、shm 立刻從 ipcs 列表消失。
static volatile sig_atomic_t g_should_exit = 0;
static void term_handler(int) {
    g_should_exit = 1;
}

// 已 fork 出去的 child PID 列表 — 只在 main loop(主執行緒/parent)裡讀寫。
// 不可用 kill(0, ...) 廣播 pgroup,那會把外部 shell / sshd session 一併殺掉;
// 必須對每個 child PID 個別 kill。SIGCHLD handler 不操作這個 vector,維持 signal safety。
static vector<pid_t> g_children;

// Per-client state
struct Command {
    vector<string> argv;
    string output_file;
    int  pipe_num        = 0;   // N for |N / !N
    bool is_err          = false;
    int  user_pipe_to    = 0;   // >n
    int  user_pipe_from  = 0;   // <n
};

struct Pipeline { vector<Command> cmds; };

struct ClientState {
    int  id        = 0;
    int  fd        = -1;
    int  inbox_fd  = -1;      // read fd for my inbox FIFO (O_RDWR keeps open)
    char nickname[MAX_NICK_LEN + 1] = {0};
    char ip[INET_ADDRSTRLEN] = {0};
    int  port      = 0;
    int  cmd_count = 0;
    map<int, array<int, 2>> pipe_table;
    map<string, string> env;
    string inbuf;
    vector<int> blacklist;
    // O_RDONLY|O_NONBLOCK fds for user-pipe FIFOs we wrote to. Holding a read
    // reference keeps the kernel pipe buffer alive after our cat child closes
    // the write end, so the receiver can still read the data later. Closed
    // at logout (or when cat is forked, in the child copy).
    vector<int> lingering_fifo_fds;
};

static void sigchld_handler(int) {
    int saved = errno;
    while (waitpid(-1, nullptr, WNOHANG) > 0) {}
    errno = saved;
}

static void write_to_fd(int fd, const string &s) {
    if (fd < 0 || s.empty()) return;
    const char *p = s.data();
    size_t left = s.size();
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) { if (errno == EINTR) continue; return; }
        p += n; left -= (size_t)n;
    }
}

static string inbox_path(int id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/inbox_%d", USER_PIPE_DIR, id);
    return buf;
}

static string user_pipe_path(int sender, int recver) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/%d_%d", USER_PIPE_DIR, sender, recver);
    return buf;
}

static bool file_exists(const string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

// Write message to a user by ID via their inbox FIFO.
// FIFO open is O_NONBLOCK so we never block if reader has gone away.
static void write_to_inbox(int target_id, const string &s) {
    string path = inbox_path(target_id);
    int fd = open(path.c_str(), O_WRONLY | O_NONBLOCK);
    if (fd < 0) return;
    write_to_fd(fd, s);
    close(fd);
}

// Broadcast to all active users; write directly to own socket, inbox for others.
// Snapshots the active user list under sem then releases the lock before I/O,
// so we never hold the semaphore across blocking writes.
static void broadcast(ClientState &cs, const string &s) {
    vector<int> targets;
    bool include_self = false;

    sem_lock();
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_shm->users[i].active) continue;
        int uid = g_shm->users[i].id;
        if (uid == cs.id) include_self = true;
        else              targets.push_back(uid);
    }
    sem_unlock();

    if (include_self) write_to_fd(cs.fd, s);
    for (int uid : targets) write_to_inbox(uid, s);
}

static void send_to_user(ClientState &cs, const string &s) {
    write_to_fd(cs.fd, s);
}

// Send to specific user: direct if self, inbox otherwise.
static void send_to_id(ClientState &cs, int target_id, const string &s) {
    if (target_id == cs.id)
        write_to_fd(cs.fd, s);
    else
        write_to_inbox(target_id, s);
}

static void send_prompt(ClientState &cs) { write_to_fd(cs.fd, "% "); }

static void send_welcome(ClientState &cs) {
    write_to_fd(cs.fd,
        "****************************************\n"
        "** Welcome to the information server. **\n"
        "****************************************\n");
}

static void drain_inbox(ClientState &cs) {
    if (cs.inbox_fd < 0) return;
    char buf[4096];
    ssize_t n;
    while ((n = read(cs.inbox_fd, buf, sizeof(buf))) > 0)
        write_to_fd(cs.fd, string(buf, (size_t)n));
}

// Shared-memory helpers — caller MUST hold the semaphore.
static UserEntry *find_user_by_id(int id) {
    if (id < 1 || id > MAX_USERS) return nullptr;
    UserEntry &u = g_shm->users[id - 1];
    return u.active ? &u : nullptr;
}

static UserEntry *find_user_by_name(const string &n) {
    for (int i = 0; i < MAX_USERS; i++)
        if (g_shm->users[i].active && n == g_shm->users[i].nickname)
            return &g_shm->users[i];
    return nullptr;
}

// Snapshot helpers — take own lock, copy data out so caller can release sem.
static bool snapshot_user_by_id(int id, UserEntry &out) {
    sem_lock();
    UserEntry *u = find_user_by_id(id);
    bool ok = (u != nullptr);
    if (ok) out = *u;
    sem_unlock();
    return ok;
}

static void cleanup_user_pipes(int id) {
    DIR *d = opendir(USER_PIPE_DIR);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != nullptr) {
        if (de->d_name[0] == '.') continue;
        if (strncmp(de->d_name, "inbox_", 6) == 0) continue; // skip inboxes
        int s = 0, r = 0;
        if (sscanf(de->d_name, "%d_%d", &s, &r) != 2) continue;
        if (s == id || r == id) {
            string p = string(USER_PIPE_DIR) + "/" + de->d_name;
            unlink(p.c_str());
        }
    }
    closedir(d);
}

static bool valid_nickname(const string &n) {
    if (n.empty() || n.size() > (size_t)MAX_NICK_LEN) return false;
    for (char c : n) if (!isalnum((unsigned char)c)) return false;
    return true;
}

// Tokenize / Parse

// Returns N if op is "|N" or "!N", sets is_err for "!N"; returns -1 otherwise.
static int isNumberedPipe(const string &op, bool &is_err) {
    if (op.size() < 2 || (op[0] != '|' && op[0] != '!')) return -1;
    for (size_t i = 1; i < op.size(); i++)
        if (!isdigit((unsigned char)op[i])) return -1;
    is_err = (op[0] == '!');
    return atoi(op.c_str() + 1);
}

// Returns >0 for ">N" (user pipe write), -1 for ">" (file redirect), 0 for neither.
static int parseGtUserPipe(const string &tok) {
    if (tok.empty() || tok[0] != '>') return 0;
    if (tok.size() == 1) return -1;
    for (size_t i = 1; i < tok.size(); i++)
        if (!isdigit((unsigned char)tok[i])) return 0;
    return atoi(tok.c_str() + 1);
}

// Returns >0 for "<N" (user pipe read), 0 for neither.
static int parseLtUserPipe(const string &tok) {
    if (tok.size() < 2 || tok[0] != '<') return 0;
    for (size_t i = 1; i < tok.size(); i++)
        if (!isdigit((unsigned char)tok[i])) return 0;
    return atoi(tok.c_str() + 1);
}

static vector<string> tokenize(const string &line) {
    vector<string> args;
    std::istringstream ss(line);
    string t;
    while (ss >> t) args.push_back(t);
    return args;
}

static vector<char *> to_argv(vector<string> &args) {
    vector<char *> v;
    v.reserve(args.size() + 1);
    for (auto &s : args) v.push_back(&s[0]);
    v.push_back(nullptr);
    return v;
}

static Pipeline parse(const vector<string> &args) {
    Pipeline p;
    Command cmd;
    for (size_t i = 0; i < args.size(); i++) {
        const string &a = args[i];
        bool is_err = false;
        int np     = isNumberedPipe(a, is_err);
        int up_to  = parseGtUserPipe(a);
        int up_frm = parseLtUserPipe(a);
        if (np > 0) {
            cmd.pipe_num = np; cmd.is_err = is_err;
            p.cmds.push_back(cmd); cmd = Command{};
        } else if (a == "|") {
            p.cmds.push_back(cmd); cmd = Command{};
        } else if (up_to == -1) {
            if (i + 1 < args.size()) cmd.output_file = args[++i];
        } else if (up_to > 0) {
            cmd.user_pipe_to = up_to;
        } else if (up_frm > 0) {
            cmd.user_pipe_from = up_frm;
        } else {
            cmd.argv.push_back(a);
        }
    }
    if (!cmd.argv.empty() || cmd.pipe_num > 0 ||
        cmd.user_pipe_to > 0 || cmd.user_pipe_from > 0 ||
        !cmd.output_file.empty())
        p.cmds.push_back(cmd);
    return p;
}

static void apply_env(ClientState &cs) {
    for (auto &kv : cs.env) setenv(kv.first.c_str(), kv.second.c_str(), 1);
}

// User-pipe I/O — proper FIFO (mkfifo) per spec.
//
// Sender (open_user_pipe_write):
//   1. mkfifo() creates the named pipe.
//   2. Open an O_RDONLY|O_NONBLOCK fd ("fd_keep"). NONBLOCK avoids waiting
//      for a writer; same-process writers count for the WR open below.
//      We stash this fd in cs.lingering_fifo_fds so the kernel pipe buffer
//      survives after our cat child exits and closes the write end.
//   3. Open O_WRONLY fd_w. Doesn't block because fd_keep is an open reader.
//      cat's stdout will be dup2'd from fd_w.
//
// Receiver (open_user_pipe_read):
//   open() with O_RDONLY|O_NONBLOCK to avoid waiting for a writer (the
//   sender already closed its WR end). Then fcntl() back to blocking mode so
//   the exec'd command (cat / number / etc.) sees normal blocking-read
//   semantics — including a real EOF (read=0) once the buffer drains, since
//   no writers exist (fd_keep is RDONLY and doesn't count as one).
static int open_user_pipe_write(ClientState &cs, const string &path) {
    if (mkfifo(path.c_str(), 0644) < 0 && errno != EEXIST) return -1;
    int fd_keep = open(path.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd_keep < 0) return -1;
    int fd_w = open(path.c_str(), O_WRONLY);
    if (fd_w < 0) { close(fd_keep); return -1; }
    cs.lingering_fifo_fds.push_back(fd_keep);
    return fd_w;
}
static int open_user_pipe_read(const string &path) {
    int fd = open(path.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd < 0) return -1;
    int flags = fcntl(fd, F_GETFL);
    if (flags >= 0) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    return fd;
}

// Execution
static void execute(ClientState &cs, Pipeline pipeline, const string &whole_line) {
    int n = (int)pipeline.cmds.size();
    if (n == 0) return;

    // Extract pipeline-level user pipe annotations.
    int up_from = 0, up_to = 0;
    for (auto &c : pipeline.cmds) {
        if (c.user_pipe_from && !up_from) up_from = c.user_pipe_from;
        if (c.user_pipe_to   && !up_to)   up_to   = c.user_pipe_to;
        c.user_pipe_from = 0;
        c.user_pipe_to   = 0;
    }

    // Validate <n and >n; errors in order: <n first, then >n.
    int up_in_fd  = -1;
    int up_out_fd = -1;
    bool up_from_ok = false;
    bool up_to_ok   = false;
    UserEntry sender_snap{}, recv_snap{};
    bool sender_ok = false, recv_ok = false;

    if (up_from > 0) {
        sender_ok = snapshot_user_by_id(up_from, sender_snap);
        if (!sender_ok) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "*** Error: user #%d does not exist yet. ***\n", up_from);
            send_to_user(cs, buf);
        } else {
            string p = user_pipe_path(up_from, cs.id);
            if (!file_exists(p)) {
                char buf[128];
                snprintf(buf, sizeof(buf),
                         "*** Error: the pipe #%d->#%d does not exist yet. ***\n",
                         up_from, cs.id);
                send_to_user(cs, buf);
            } else {
                up_in_fd = open_user_pipe_read(p);
                if (up_in_fd >= 0) { up_from_ok = true; unlink(p.c_str()); }
            }
        }
    }

    if (up_to > 0) {
        recv_ok = snapshot_user_by_id(up_to, recv_snap);
        if (!recv_ok) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "*** Error: user #%d does not exist yet. ***\n", up_to);
            send_to_user(cs, buf);
        } else {
            string p = user_pipe_path(cs.id, up_to);
            if (file_exists(p)) {
                char buf[128];
                snprintf(buf, sizeof(buf),
                         "*** Error: the pipe #%d->#%d already exists. ***\n",
                         cs.id, up_to);
                send_to_user(cs, buf);
            } else {
                up_to_ok = true;
            }
        }
    }

    // Broadcasts: receive first, then send (per spec ordering).
    if (up_from_ok) {
        char buf[8192];
        snprintf(buf, sizeof(buf),
                 "*** %s (#%d) just received from %s (#%d) by '%s' ***\n",
                 cs.nickname, cs.id, sender_snap.nickname, sender_snap.id,
                 whole_line.c_str());
        broadcast(cs, buf);
    }
    if (up_to_ok) {
        string p = user_pipe_path(cs.id, up_to);
        up_out_fd = open_user_pipe_write(cs, p);
        if (up_out_fd < 0) {
            up_to_ok = false;
        } else {
            char buf[8192];
            snprintf(buf, sizeof(buf),
                     "*** %s (#%d) just piped '%s' to %s (#%d) ***\n",
                     cs.nickname, cs.id, whole_line.c_str(),
                     recv_snap.nickname, recv_snap.id);
            broadcast(cs, buf);
        }
    }

    int prev_read_fd = -1;
    bool new_segment = true;
    vector<pid_t> pids;

    for (int i = 0; i < n; i++) {
        Command &cmd = pipeline.cmds[i];

        int read_fd = -1;
        if (new_segment) {
            cs.cmd_count++;
            new_segment = false;
            if (cs.pipe_table.count(cs.cmd_count)) {
                read_fd = cs.pipe_table[cs.cmd_count][0];
                close(cs.pipe_table[cs.cmd_count][1]);
                cs.pipe_table.erase(cs.cmd_count);
            }
        }

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

        int write_fd    = STDOUT_FILENO;
        int err_fd      = STDERR_FILENO;
        int next_read_fd = -1;

        if (cmd.pipe_num > 0) {
            int target = cs.cmd_count + cmd.pipe_num;
            if (!cs.pipe_table.count(target)) {
                int fd[2];
                if (pipe(fd) < 0) { perror("pipe"); return; }
                cs.pipe_table[target] = {fd[0], fd[1]};
            }
            new_segment = true;
            write_fd = cs.pipe_table[target][1];
            if (cmd.is_err) err_fd = write_fd;
        } else if (i < n - 1) {
            int fd[2];
            if (pipe(fd) < 0) { perror("pipe"); return; }
            write_fd     = fd[1];
            next_read_fd = fd[0];
        } else {
            if (up_out_fd >= 0) write_fd = up_out_fd;
        }

        pid_t pid;
        while ((pid = fork()) < 0) { int st; waitpid(-1, &st, WNOHANG); sleep(0); }

        if (pid == 0) {
            // Child
            signal(SIGCHLD, SIG_DFL);
            signal(SIGPIPE, SIG_DFL);
            apply_env(cs);

            if (g_listen_fd >= 0) close(g_listen_fd);
            if (cs.inbox_fd >= 0) close(cs.inbox_fd);
            // Don't leak FIFO read-keepers into the exec'd command. The parent
            // still holds its copies, so the FIFO buffers stay alive.
            for (int fd : cs.lingering_fifo_fds) close(fd);

            if (read_fd  != STDIN_FILENO)  dup2(read_fd,  STDIN_FILENO);
            if (write_fd != STDOUT_FILENO) dup2(write_fd, STDOUT_FILENO);
            if (err_fd   != STDERR_FILENO) dup2(err_fd,   STDERR_FILENO);

            if (read_fd  != STDIN_FILENO)  close(read_fd);
            if (write_fd != STDOUT_FILENO) close(write_fd);
            if (err_fd   != STDERR_FILENO && err_fd != write_fd) close(err_fd);
            if (next_read_fd != -1) close(next_read_fd);

            for (auto const &kv : cs.pipe_table) {
                close(kv.second[0]);
                close(kv.second[1]);
            }
            if (up_out_fd >= 0 && write_fd != up_out_fd) close(up_out_fd);

            // Route stdout/stderr to socket when not redirected.
            if (write_fd == STDOUT_FILENO) {
                if (up_to > 0) {
                    int dn = open("/dev/null", O_WRONLY);
                    if (dn >= 0) { dup2(dn, STDOUT_FILENO); close(dn); }
                } else {
                    dup2(cs.fd, STDOUT_FILENO);
                }
            }
            if (err_fd == STDERR_FILENO) dup2(cs.fd, STDERR_FILENO);
            if (read_fd == STDIN_FILENO) {
                int dn = open("/dev/null", O_RDONLY);
                if (dn >= 0) { dup2(dn, STDIN_FILENO); close(dn); }
            }
            close(cs.fd);

            if (!cmd.output_file.empty()) {
                int f = open(cmd.output_file.c_str(),
                             O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (f < 0) { perror("open"); _exit(1); }
                dup2(f, STDOUT_FILENO);
                close(f);
            }

            if (cmd.argv.empty()) _exit(0);

            // On some Linux kernels (e.g. OrbStack's Ubuntu) a dup2'd file
            // descriptor must be touched by a syscall before exec, or the
            // exec'd program sees EBADF on it. fstat is the lightest probe.
            if (read_fd != STDIN_FILENO) {
                struct stat _st;
                (void)fstat(STDIN_FILENO, &_st);
            }

            vector<char *> argv = to_argv(cmd.argv);
            execvp(argv[0], argv.data());
            fprintf(stderr, "Unknown command: [%s].\n", argv[0]);
            _exit(1);
        }

        // Parent
        if (cmd.pipe_num == 0) pids.push_back(pid);
        if (read_fd != STDIN_FILENO) close(read_fd);
        if (i < n - 1 && cmd.pipe_num == 0) {
            close(write_fd);
            prev_read_fd = next_read_fd;
        } else {
            if (up_out_fd >= 0 && write_fd == up_out_fd) {
                close(up_out_fd);
                up_out_fd = -1;
            }
            prev_read_fd = -1;
        }
    }

    if (up_in_fd  >= 0) close(up_in_fd);
    if (up_out_fd >= 0) close(up_out_fd);
    for (pid_t pid : pids) waitpid(pid, nullptr, 0);
}

// Built-in functions
static void consume_cmd_slot(ClientState &cs) {
    cs.cmd_count++;
    auto it = cs.pipe_table.find(cs.cmd_count);
    if (it != cs.pipe_table.end()) {
        close(it->second[0]);
        close(it->second[1]);
        cs.pipe_table.erase(it);
    }
}

static void do_who(ClientState &cs) {
    string out = "<ID>\t<fd>\t<nickname>\t<IP:port>\t<indicate me>\n";
    sem_lock();
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_shm->users[i].active) continue;
        UserEntry &u = g_shm->users[i];
        char row[256];
        snprintf(row, sizeof(row), "%d\t%d\t%s\t%s:%d%s\n",
                 u.id, u.fd, u.nickname, u.ip, u.port,
                 (u.id == cs.id) ? "\t<-me" : "");
        out += row;
    }
    sem_unlock();
    send_to_user(cs, out);
    consume_cmd_slot(cs);
}

static void do_name(ClientState &cs, const string &nn) {
    if (!valid_nickname(nn)) { consume_cmd_slot(cs); return; }

    bool exists = false;
    sem_lock();
    if (find_user_by_name(nn)) {
        exists = true;
    } else {
        // Atomic: take name + write back to shared memory under same lock.
        snprintf(cs.nickname, sizeof(cs.nickname), "%s", nn.c_str());
        UserEntry *ue = find_user_by_id(cs.id);
        if (ue) snprintf(ue->nickname, sizeof(ue->nickname), "%s", nn.c_str());
    }
    sem_unlock();

    if (exists) {
        char buf[128];
        snprintf(buf, sizeof(buf), "*** User '%s' already exists. ***\n", nn.c_str());
        send_to_user(cs, buf);
        consume_cmd_slot(cs);
        return;
    }

    char buf[256];
    snprintf(buf, sizeof(buf), "*** User from %s:%d is named '%s'. ***\n",
             cs.ip, cs.port, cs.nickname);
    broadcast(cs, buf);   // self-locks
    consume_cmd_slot(cs);
}

static void do_yell(ClientState &cs, const string &msg) {
    char buf[2048];
    snprintf(buf, sizeof(buf), "*** %s yelled ***: %s\n", cs.nickname, msg.c_str());

    // Snapshot active users，且過濾掉與自己有封鎖關係的 user。
    vector<int> targets;
    bool include_self = false;
    sem_lock();
    int my_idx = cs.id - 1;
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_shm->users[i].active) continue;
        int uid = g_shm->users[i].id;
        if (uid == cs.id) { include_self = true; continue; }
        int t_idx = uid - 1;
        // 任一方有封鎖 → 跳過
        if (g_shm->blocked[my_idx][t_idx] || g_shm->blocked[t_idx][my_idx]) continue;
        targets.push_back(uid);
    }
    sem_unlock();

    if (include_self) write_to_fd(cs.fd, buf);
    for (int uid : targets) write_to_inbox(uid, buf);

    consume_cmd_slot(cs);
}

static void do_tell(ClientState &cs, int target_id, const string &msg) {
    UserEntry t{};
    bool exists = false;
    bool blocked = false;
    sem_lock();
    UserEntry *tu = find_user_by_id(target_id);
    if (tu) {
        exists = true;
        t = *tu;
        if (cs.id >= 1 && target_id >= 1 && target_id <= MAX_USERS) {
            int s = cs.id - 1, tg = target_id - 1;
            blocked = g_shm->blocked[s][tg] || g_shm->blocked[tg][s];
        }
    }
    sem_unlock();

    if (!exists) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "*** Error: user #%d does not exist yet. ***\n", target_id);
        send_to_user(cs, buf);
        consume_cmd_slot(cs);
        return;
    }
    if (blocked) {
        // 任一方封鎖時靜默丟棄，target 收不到。
        consume_cmd_slot(cs);
        return;
    }
    char buf[2048];
    snprintf(buf, sizeof(buf), "*** %s told you ***: %s\n", cs.nickname, msg.c_str());
    send_to_id(cs, target_id, buf);
    consume_cmd_slot(cs);
}

// "block <nickname>" — 把 nickname 對應的 user 加入自己的封鎖名單。
// 封鎖後 yell / tell 雙向都看不到。沿用「對稱效果」設計，因此只要單向設一次即可。
static void do_block(ClientState &cs, const string &nn) {
    if (nn.empty()) { consume_cmd_slot(cs); return; }

    bool ok = false;
    sem_lock();
    UserEntry *tu = find_user_by_name(nn);
    if (tu && tu->id != cs.id) {
        g_shm->blocked[cs.id - 1][tu->id - 1] = true;
        ok = true;
    }
    sem_unlock();

    if (!ok) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "*** Error: user '%s' does not exist. ***\n", nn.c_str());
        send_to_user(cs, buf);
    }
    consume_cmd_slot(cs);
}

// Line processing
static bool process_line(ClientState &cs, const string &raw) {
    string line = raw;
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
        line.pop_back();
    if (line.empty()) return false;

    vector<string> tokens = tokenize(line);
    if (tokens.empty()) return false;

    const string &cmd0 = tokens[0];

    if (cmd0 == "exit") return true;

    if (cmd0 == "setenv" && tokens.size() >= 3) {
        cs.env[tokens[1]] = tokens[2];
        consume_cmd_slot(cs);
        return false;
    }
    if (cmd0 == "printenv" && tokens.size() >= 2) {
        auto it = cs.env.find(tokens[1]);
        if (it != cs.env.end()) send_to_user(cs, it->second + "\n");
        consume_cmd_slot(cs);
        return false;
    }
    if (cmd0 == "who")  { do_who(cs); return false; }
    if (cmd0 == "name") {
        do_name(cs, tokens.size() >= 2 ? tokens[1] : string());
        return false;
    }
    if (cmd0 == "yell") {
        size_t pos = line.find_first_not_of(" \t");
        pos = line.find_first_of(" \t", pos);
        string msg;
        if (pos != string::npos) {
            size_t s = line.find_first_not_of(" \t", pos);
            if (s != string::npos) msg = line.substr(s);
        }
        do_yell(cs, msg);
        return false;
    }
    if (cmd0 == "tell" && tokens.size() >= 2) {
        int target = atoi(tokens[1].c_str());
        size_t p = line.find_first_not_of(" \t");
        p = line.find_first_of(" \t", p);
        p = line.find_first_not_of(" \t", p);
        p = line.find_first_of(" \t", p);
        string msg;
        if (p != string::npos) {
            size_t s = line.find_first_not_of(" \t", p);
            if (s != string::npos) msg = line.substr(s);
        }
        do_tell(cs, target, msg);
        return false;
    }

    if (cmd0 == "block" && tokens.size() >= 2) {
        do_block(cs, tokens[1]);
        return false;
    }

    Pipeline pl = parse(tokens);
    execute(cs, pl, line);
    return false;
}

// Child main
static void child_main(int sock_fd, const sockaddr_in &addr) {
    if (g_listen_fd >= 0) { close(g_listen_fd); g_listen_fd = -1; }

    signal(SIGPIPE, SIG_IGN);
    {
        struct sigaction sa;
        sa.sa_handler = sigchld_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, nullptr);
    }
    // Children must NOT clean up parent's IPC on exit.
    signal(SIGINT,  SIG_DFL);
    signal(SIGTERM, SIG_DFL);

    ClientState cs;
    cs.fd = sock_fd;
    inet_ntop(AF_INET, &addr.sin_addr, cs.ip, sizeof(cs.ip));
    cs.port = ntohs(addr.sin_port);
    strcpy(cs.nickname, "(no name)");
    cs.env["PATH"] = "bin:.";

    // Find a free slot — but DON'T mark active yet; we need the inbox FIFO
    // ready first so that broadcasters never miss messages addressed to us.
    sem_lock();
    int slot = -1;
    for (int i = 0; i < MAX_USERS; i++) {
        if (!g_shm->users[i].active) { slot = i; break; }
    }
    sem_unlock();

    if (slot < 0) {
        write(sock_fd, "Server full.\n", 13);
        close(sock_fd);
        _exit(0);
    }
    cs.id = slot + 1;

    // Create inbox FIFO BEFORE marking the slot active so writers cannot
    // race us into a "user is active but FIFO doesn't exist" window.
    string ipath = inbox_path(cs.id);
    unlink(ipath.c_str());
    mkfifo(ipath.c_str(), 0644);
    cs.inbox_fd = open(ipath.c_str(), O_RDWR | O_NONBLOCK);

    // Now atomically claim the slot. Re-check active in case another fork
    // grabbed the same index while we were preparing the FIFO.
    sem_lock();
    if (g_shm->users[slot].active) {
        // Slot was taken; pick another and rebuild inbox if needed.
        int new_slot = -1;
        for (int i = 0; i < MAX_USERS; i++) {
            if (!g_shm->users[i].active) { new_slot = i; break; }
        }
        if (new_slot < 0) {
            sem_unlock();
            if (cs.inbox_fd >= 0) close(cs.inbox_fd);
            unlink(ipath.c_str());
            write(sock_fd, "Server full.\n", 13);
            close(sock_fd);
            _exit(0);
        }
        slot = new_slot;
        cs.id = slot + 1;
        // Move FIFO to new id.
        sem_unlock();
        if (cs.inbox_fd >= 0) close(cs.inbox_fd);
        unlink(ipath.c_str());
        ipath = inbox_path(cs.id);
        unlink(ipath.c_str());
        mkfifo(ipath.c_str(), 0644);
        cs.inbox_fd = open(ipath.c_str(), O_RDWR | O_NONBLOCK);
        sem_lock();
    }
    UserEntry &ue = g_shm->users[slot];
    ue.active = true;
    ue.id     = cs.id;
    ue.fd     = sock_fd;
    snprintf(ue.nickname, sizeof(ue.nickname), "%s", cs.nickname);
    snprintf(ue.ip, sizeof(ue.ip), "%s", cs.ip);
    ue.port   = cs.port;
    ue.pid    = getpid();
    // 清掉這個 slot 的封鎖紀錄（雙向）——避免上次坐這個 slot 的人留下舊狀態。
    for (int i = 0; i < MAX_USERS; i++) {
        g_shm->blocked[slot][i] = false;
        g_shm->blocked[i][slot] = false;
    }
    sem_unlock();

    // Welcome → login broadcast → prompt.
    send_welcome(cs);
    {
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "*** User '(no name)' entered from %s:%d. ***\n", cs.ip, cs.port);
        broadcast(cs, buf);   // broadcast self-locks
    }
    send_prompt(cs);

    // Main select loop
    for (;;) {
        fd_set rset;
        FD_ZERO(&rset);
        FD_SET(sock_fd, &rset);
        int maxfd = sock_fd;
        if (cs.inbox_fd >= 0) {
            FD_SET(cs.inbox_fd, &rset);
            if (cs.inbox_fd > maxfd) maxfd = cs.inbox_fd;
        }

        int r = select(maxfd + 1, &rset, nullptr, nullptr, nullptr);
        if (r < 0) { if (errno == EINTR) continue; break; }

        if (cs.inbox_fd >= 0 && FD_ISSET(cs.inbox_fd, &rset))
            drain_inbox(cs);

        if (!FD_ISSET(sock_fd, &rset)) continue;

        char tmp[4096];
        ssize_t n = read(sock_fd, tmp, sizeof(tmp));
        if (n == 0 || (n < 0 && errno != EINTR && errno != EAGAIN)) break;
        if (n <= 0) continue;

        cs.inbuf.append(tmp, (size_t)n);
        bool disconnect = false;
        size_t pos;
        while ((pos = cs.inbuf.find('\n')) != string::npos) {
            string line = cs.inbuf.substr(0, pos);
            cs.inbuf.erase(0, pos + 1);

            // Each helper takes the semaphore only around shared-memory ops.
            bool quit = process_line(cs, line);

            // Pick up any broadcasts before printing the next prompt.
            drain_inbox(cs);

            if (quit) { disconnect = true; break; }
            send_prompt(cs);
        }
        if (disconnect) break;
    }

    // Cleanup
    char lbuf[128];
    snprintf(lbuf, sizeof(lbuf), "*** User '%s' left. ***\n", cs.nickname);

    // Atomically mark inactive so broadcast() excludes us.
    sem_lock();
    for (int i = 0; i < MAX_USERS; i++) {
        if (g_shm->users[i].active && g_shm->users[i].id == cs.id) {
            g_shm->users[i].active = false;
            break;
        }
    }
    sem_unlock();

    broadcast(cs, lbuf);   // self-locks; we're already inactive so excluded

    if (cs.inbox_fd >= 0) close(cs.inbox_fd);
    unlink(ipath.c_str());
    for (auto &kv : cs.pipe_table) { close(kv.second[0]); close(kv.second[1]); }
    // Release any FIFO read-keepers we held to keep user-pipe data alive.
    for (int fd : cs.lingering_fifo_fds) close(fd);
    cleanup_user_pipes(cs.id);
    close(sock_fd);
    _exit(0);
}

int main(int argc, char *argv[]) {
    if (argc != 2) { fprintf(stderr, "Usage: %s <port>\n", argv[0]); return 1; }
    int port = atoi(argv[1]);

    // Prepare user_pipe directory.
    if (mkdir(USER_PIPE_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir user_pipe"); return 1;
    }
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

    // Shared memory.
    g_shm_id = shmget(IPC_PRIVATE, sizeof(SharedMem), IPC_CREAT | 0666);
    if (g_shm_id < 0) { perror("shmget"); return 1; }
    g_shm = (SharedMem *)shmat(g_shm_id, nullptr, 0);
    if (g_shm == (void *)-1) { perror("shmat"); return 1; }
    memset(g_shm, 0, sizeof(SharedMem));

    // Semaphore (initial value = 1).
    g_sem_id = semget(IPC_PRIVATE, 1, IPC_CREAT | 0666);
    if (g_sem_id < 0) { perror("semget"); cleanup_ipc(); return 1; }
    if (semctl(g_sem_id, 0, SETVAL, 1) < 0) { perror("semctl"); cleanup_ipc(); return 1; }

    // Make sure shm/sem are released no matter how we exit.
    atexit(cleanup_ipc);
    {
        struct sigaction sa = {};       // 必須 zero-init,否則 sa_restorer 等欄位會是垃圾
        sa.sa_handler = term_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;                // 不要 SA_RESTART:這樣 accept 會被 EINTR 中斷
        sigaction(SIGINT,  &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        sigaction(SIGHUP,  &sa, nullptr);
    }

    signal(SIGPIPE, SIG_IGN);
    {
        struct sigaction sa = {};
        sa.sa_handler = sigchld_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, nullptr);
    }

    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (::bind(g_listen_fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(g_listen_fd, 32) < 0) { perror("listen"); return 1; }

    for (;;) {
        sockaddr_in cli{};
        socklen_t clilen = sizeof(cli);
        int cfd = accept(g_listen_fd, (sockaddr *)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) {
                // 被 SIGINT/TERM/HUP 之類打斷:看是否該退出
                if (g_should_exit) break;
                continue;          // 例如被 SIGCHLD 打斷,正常 retry
            }
            perror("accept"); break;
        }

        pid_t pid = fork();
        if (pid < 0) { perror("fork"); close(cfd); continue; }
        if (pid == 0) {
            child_main(cfd, cli);
            _exit(0);
        }
        g_children.push_back(pid);   // 記下 child PID,shutdown 時逐一 SIGTERM
        close(cfd);
    }

    // ── Graceful shutdown ────────────────────────────────────────────────
    // 收到 SIGINT/TERM 後 main loop break 到這裡。
    // 把所有還活著的 child 個別 SIGTERM(不能用 kill(0, ...),那會殃及外部 shell),
    // 再 waitpid 全部 reap 完;parent 此時也會自動 detach shm,然後才 cleanup_ipc。
    // 這樣 shm 的 nattch 確實歸零,segment 真的從 ipcs 列表消失(不留 'dest' 殭屍)。

    close(g_listen_fd);

    for (pid_t p : g_children) kill(p, SIGTERM);  // 已死的 child 會回 ESRCH,無害

    // 等所有 child reap 完。最多等 5 秒,避免異常情況下卡住整個 cleanup。
    for (int i = 0; i < 50; ++i) {
        bool reaped_any = false;
        for (;;) {
            pid_t r = waitpid(-1, nullptr, WNOHANG);
            if (r > 0) { reaped_any = true; continue; }
            if (r == 0) break;                 // 還有 child 在跑但還沒 ready
            if (errno == ECHILD) goto reaped;  // 沒任何 child 了
            break;                              // 其他錯誤:放棄
        }
        if (!reaped_any) usleep(100000);       // 100ms,讓 child 有時間死乾淨
    }
reaped:

    cleanup_ipc();   // 此時 nattch == 0,IPC_RMID 立刻完成釋放
    return 0;
}

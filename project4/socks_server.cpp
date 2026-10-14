#include <boost/asio/io_context.hpp>
#include <iostream>
#include <arpa/inet.h>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <csignal>
#include <string>
#include <fstream>
#include <vector>
#include <utility>
#include <boost/asio.hpp>

namespace asio = boost::asio;
using namespace std;
using boost::asio::ip::tcp;
using boost::asio::ip::address_v4;

struct LogInfo {
    string s_ip;
    uint16_t s_port = 0;
    string d_ip;
    uint16_t d_port = 0;
    string command;
};

static void print_log(LogInfo & li, bool accepted) {
    ostringstream out;
    out << "<S_IP>: " << li.s_ip << "\n"
        << "<S_PORT>: " << li.s_port << "\n"
        << "<D_IP>: " << li.d_ip << "\n"
        << "<D_PORT>: " << li.d_port << "\n"
        << "<Command>: " << li.command << "\n"
        << "<Reply>: " << (accepted ? "Accept" : "Reject") << "\n\n";
    cout << out.str();
    cout.flush();
}

// --- Firewall ---
struct Rule {
    char kind;                  // 'c' = CONNECT, 'b' = BIND
    array<int, 4> oct;        // -1 = wildcard, otherwise 0..255
};

static vector<Rule> load_rules(const string& path) {
    vector<Rule> rules;
    ifstream f(path);
    if (!f) return rules;
    string line;
    while (getline(f, line)) {
        auto h = line.find('#');
        if (h != string::npos) line.erase(h);
        istringstream iss(line);
        string verb, kind_s, ip;
        if (!(iss >> verb >> kind_s >> ip)) continue;
        if (verb != "permit" || kind_s.size() != 1) continue;
        char kind = kind_s[0];
        if (kind != 'c' && kind != 'b') continue;
        Rule r{kind, {-1, -1, -1, -1}};
        size_t pos = 0;
        int oc = 0;
        bool ok = true;
        while (oc < 4) {
            size_t dot = ip.find('.', pos);
            string tok = ip.substr(pos, dot == string::npos ? string::npos : dot - pos);
            if (tok == "*") {
                r.oct[oc] = -1;
            } else {
                try {
                    int v = stoi(tok);
                    if (v < 0 || v > 255) {
                        ok = false;
                        break;
                    }
                    r.oct[oc] = v;
                } catch (...) {
                    ok = false;
                    break;
                }
            }
            oc++;
            if (dot == string::npos) break;
            pos = dot + 1;
        }
        if (!ok || oc != 4) continue;
        rules.push_back(r);
    }
    return rules;
}

static bool allowed(const vector<Rule>& rules, char kind, const address_v4& a) {
    auto b = a.to_bytes();
    for (const auto& r : rules) {
        if (r.kind != kind) continue;
        bool m = true;
        for (int i = 0; i < 4; i++) {
            if (r.oct[i] == -1) continue;
            if (r.oct[i] != static_cast<int>(b[i])) {
                m = false;
                break;
            }
        }
        if (m) return true;
    }
    return false;
}

// Request
//         +----+----+----+----+----+----+----+----+----+----+....+----+
//         | VN | CD | DSTPORT |      DSTIP        | USERID       |NULL|
//         +----+----+----+----+----+----+----+----+----+----+....+----+
// bytes:    1    1       2              4          variable        1

// Reply
//         +----+----+----+----+----+----+----+----+
//         | VN | CD | DSTPORT |       DSTIP       |
//         +----+----+----+----+----+----+----+----+
// bytes:    1     1      2              4

// Connect VN, DPORT, DIP can be set to 0
static void send_reply(tcp::socket &sock, uint8_t cd, uint16_t port = 0, uint32_t ip = 0) {
    array<uint8_t, 8> r{};
    r[0] = 0;
    r[1] = cd;
    r[2] = (port >> 8) & 0xff;
    r[3] = port & 0xff;
    r[4] = (ip >> 24) & 0xff;
    r[5] = (ip >> 16) & 0xff;
    r[6] = (ip >> 8) & 0xff;
    r[7] = ip & 0xff;
    boost::system::error_code ec;
    asio::write(sock, asio::buffer(r), ec);
}

// Async bidirectonal relay
class Relay : public enable_shared_from_this<Relay> {
public:
    Relay(tcp::socket client, tcp::socket remote)
        : client_(std::move(client)), remote_(std::move(remote)) {}

    void start() {
        pump_c_to_r();
        pump_r_to_c();
    }
private:
    void pump_c_to_r() {
        auto self = shared_from_this();
        client_.async_read_some(asio::buffer(buf_c_),
        [this, self](const boost::system::error_code& ec, size_t n) {
            if (ec) { close_both(); return; }
            asio::async_write(remote_, asio::buffer(buf_c_, n),
                [this, self](const boost::system::error_code& ec2, size_t) {
                    if (ec2) { close_both(); return; }
                    pump_c_to_r();
                });
        });
    }

    void pump_r_to_c() {
        auto self = shared_from_this();
        remote_.async_read_some(asio::buffer(buf_r_),
        [this, self](const boost::system::error_code& ec, size_t n) {
            if (ec) { close_both(); return; }
            asio::async_write(client_, asio::buffer(buf_r_, n),
                [this, self](const boost::system::error_code& ec2, size_t) {
                    if (ec2) { close_both(); return; }
                    pump_r_to_c();
                });
        });
    }

    void close_both() {
        boost::system::error_code ec;
        client_.close(ec);
        remote_.close(ec);
    }

    tcp::socket client_, remote_;
    array<char, 8192> buf_c_{}, buf_r_{};
};

static void handle_client(asio::io_context& io, tcp::socket sock) {
    boost::system::error_code ec;

    uint8_t vn = 0;
    asio::read(sock, asio::buffer(&vn, 1), ec);
    // Reject SOCKS 5
    if (ec || vn != 4) return;

    array<uint8_t, 7> rest{};
    asio::read(sock, asio::buffer(rest), ec);
    if (ec) {
        return;
    }

    uint8_t cd = rest[0];
    uint16_t dport = ((uint16_t)rest[1] << 8) | rest[2];
    address_v4::bytes_type ip{{rest[3], rest[4], rest[5], rest[6]}};
    address_v4 dip(ip); // ip -> boost::asio ip object

    // string userid;
    char ch;
    while (true) {
        asio::read(sock, asio::buffer(&ch, 1), ec);
        if (ec) break;
        if (ch == '\0') break;
        // userid += ch;
    }

    bool is_4a = (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] != 0);
    string domain;
    if (is_4a) {
        while (true) {
            asio::read(sock, asio::buffer(&ch, 1), ec);
            if (ec || ch == '\0') break;
            domain += ch;
        }
    }

    LogInfo li;
    auto rep = sock.remote_endpoint(ec);
    if (!ec) {
        li.s_ip = rep.address().to_string();
        li.s_port = rep.port();
    }
    li.d_port = dport;
    li.command = (cd == 2) ? "BIND" : "CONNECT";

    if (cd != 1 && cd != 2) return; // unknown CD: silently deop

    if (cd == 2) {
        if (is_4a) {
            tcp::resolver res(io);
            auto results = res.resolve(domain, to_string(dport), ec);
            bool found = false;
            if (!ec) {
                for (const auto& e : results) {
                    if (e.endpoint().address().is_v4()) {
                        dip = e.endpoint().address().to_v4();
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                li.d_ip = domain;
                print_log(li, false);
                send_reply(sock, 91);
                return;
            }
        }
        li.d_ip = dip.to_string();

        auto rules = load_rules("socks.conf");
        if (!allowed(rules, 'b', dip)) {
            print_log(li, false);
            send_reply(sock, 91);
            return;
        }
        tcp::acceptor bind_acc(io);
        bind_acc.open(tcp::v4(), ec);
        if (!ec) {
            bind_acc.set_option(asio::socket_base::reuse_address(true));
            bind_acc.bind(tcp::endpoint((tcp::v4()), 0), ec);
        }
        if (!ec) {
            bind_acc.listen(asio::socket_base::max_listen_connections, ec);
        }
        if (ec) {
            print_log(li, false);
            send_reply(sock, 91);
            return;
        }

        uint16_t bound_port = bind_acc.local_endpoint().port();
        print_log(li, true);
        send_reply(sock, 90, bound_port, 0);

        // wait for FTP server connect
        tcp::socket dest(io);
        bind_acc.accept(dest, ec);
        if (ec) {
            send_reply(sock, 91);
            return;
        }

        send_reply(sock, 90, bound_port, 0); // second reply come from dest, starting relay
        bind_acc.close(ec);

        auto relay = make_shared<Relay>(std::move(sock), std::move(dest));
        relay->start();
        io.run();
        return;
    }
    if (is_4a) {
        tcp::resolver res(io);
        auto results = res.resolve(domain, to_string(dport), ec);
        bool found = false;
        if (!ec) {
            for (const auto& e : results) {
                if (e.endpoint().address().is_v4()) {
                    dip = e.endpoint().address().to_v4();
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            li.d_ip = domain;
            print_log(li, false);
            send_reply(sock, 91);
            return;
        }
    }
    li.d_ip = dip.to_string();

    auto rules = load_rules("socks.conf");
    if (!allowed(rules, 'c', dip)) {
        print_log(li, false);
        send_reply(sock, 91);
        return;
    }
    tcp::socket remote(io);
    remote.connect(tcp::endpoint(dip, dport), ec);
    if (ec) {
        print_log(li, false);
        send_reply(sock, 91);
        return;
    }
    print_log(li, true);

    // Accepted client
    send_reply(sock, 90);

    auto relay = std::make_shared<Relay>(std::move(sock), std::move(remote));
    relay->start();
    io.run();
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <port>" << endl;
        return 1;
    }

    signal(SIGCHLD, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    int port = atoi(argv[1]);

    asio::io_context io;
    tcp::acceptor acc(io, tcp::endpoint(tcp::v4(), port));

    while (true) {
        tcp::socket sock(io);
        acc.accept(sock);

        io.notify_fork(asio::io_context::fork_prepare);
        pid_t pid = fork();
        if (pid == 0) {
            // child
            io.notify_fork(asio::io_context::fork_child);
            acc.close();
            handle_client(io, std::move(sock));
            _Exit(0);
        } else if (pid > 0) {
            // parent
            io.notify_fork(asio::io_context::fork_parent);
            sock.close();
        } else {
            // fork failed
            sock.close();
        }
    }

    return 0;
}

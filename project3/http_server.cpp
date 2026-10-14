#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>

#include <boost/asio.hpp>

using boost::asio::ip::tcp;

namespace {

std::string trim(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r' || s[b] == '\n')) ++b;
    while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r' || s[e - 1] == '\n')) --e;
    return s.substr(b, e - b);
}

bool iequals_prefix(const std::string& s, const std::string& prefix) {
    if (s.size() < prefix.size()) return false;
    for (size_t i = 0; i < prefix.size(); ++i) {
        char a = s[i], b = prefix[i];
        if (a >= 'A' && a <= 'Z') a = char(a - 'A' + 'a');
        if (b >= 'A' && b <= 'Z') b = char(b - 'A' + 'a');
        if (a != b) return false;
    }
    return true;
}

class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    explicit HttpSession(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        auto self = shared_from_this();
        boost::asio::async_read_until(
            socket_, request_buf_, "\r\n\r\n",
            [this, self](const boost::system::error_code& ec, std::size_t /*n*/) {
                if (ec) return;
                handle_request();
            });
    }

private:
    void handle_request() {
        std::istream is(&request_buf_);
        std::string request_line;
        if (!std::getline(is, request_line)) return;
        if (!request_line.empty() && request_line.back() == '\r') request_line.pop_back();

        // Parse: METHOD URI PROTOCOL
        std::string method, uri, protocol;
        {
            size_t s1 = request_line.find(' ');
            if (s1 == std::string::npos) return;
            size_t s2 = request_line.find(' ', s1 + 1);
            if (s2 == std::string::npos) return;
            method   = request_line.substr(0, s1);
            uri      = request_line.substr(s1 + 1, s2 - s1 - 1);
            protocol = request_line.substr(s2 + 1);
        }
        if (method != "GET") return;

        // Split URI into path + query
        std::string path = uri, query;
        {
            size_t q = uri.find('?');
            if (q != std::string::npos) {
                path  = uri.substr(0, q);
                query = uri.substr(q + 1);
            }
        }

        // Walk header lines for Host:
        std::string host_header;
        std::string line;
        while (std::getline(is, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) break;
            if (iequals_prefix(line, "Host:")) {
                host_header = trim(line.substr(5));
            }
        }

        // Validate path: starts with '/', ends with .cgi, no traversal.
        if (path.size() < 2 || path[0] != '/') return;
        if (path.find("..") != std::string::npos) return;
        const std::string suffix = ".cgi";
        if (path.size() < suffix.size() ||
            path.compare(path.size() - suffix.size(), suffix.size(), suffix) != 0) return;

        std::string prog = "." + path;  // "./<name>.cgi"

        // Local + remote endpoints (capture before fork; child may close socket otherwise).
        boost::system::error_code ep_ec;
        auto local = socket_.local_endpoint(ep_ec);
        if (ep_ec) return;
        auto remote = socket_.remote_endpoint(ep_ec);
        if (ep_ec) return;

        const std::string server_addr = local.address().to_string();
        const std::string server_port = std::to_string(local.port());
        const std::string remote_addr = remote.address().to_string();
        const std::string remote_port = std::to_string(remote.port());

        // Status line goes out before the fork so the CGI's own headers concatenate cleanly.
        boost::system::error_code wec;
        boost::asio::write(socket_, boost::asio::buffer(std::string("HTTP/1.1 200 OK\r\n")), wec);
        if (wec) return;

        int fd = socket_.native_handle();

        pid_t pid = fork();
        if (pid < 0) {
            // fork failed
            return;
        }
        if (pid == 0) {
            // Child: set the 9 CGI env vars, dup the socket onto stdout, exec.
            setenv("REQUEST_METHOD",  method.c_str(),       1);
            setenv("REQUEST_URI",     uri.c_str(),          1);
            setenv("QUERY_STRING",    query.c_str(),        1);
            setenv("SERVER_PROTOCOL", protocol.c_str(),     1);
            setenv("HTTP_HOST",       host_header.c_str(),  1);
            setenv("SERVER_ADDR",     server_addr.c_str(),  1);
            setenv("SERVER_PORT",     server_port.c_str(),  1);
            setenv("REMOTE_ADDR",     remote_addr.c_str(),  1);
            setenv("REMOTE_PORT",     remote_port.c_str(),  1);

            if (dup2(fd, STDOUT_FILENO) < 0) _exit(1);

            char* argv[] = { const_cast<char*>(prog.c_str()), nullptr };
            execv(argv[0], argv);
            _exit(1);
        }
        // Parent: drop the socket. SIGCHLD is ignored in main() so no zombie reaping needed.
        boost::system::error_code cec;
        socket_.close(cec);
    }

    tcp::socket socket_;
    boost::asio::streambuf request_buf_;
};

class HttpServer {
public:
    HttpServer(boost::asio::io_context& io, unsigned short port)
        : acceptor_(io, tcp::endpoint(tcp::v4(), port)) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<HttpSession>(std::move(socket))->start();
                }
                do_accept();
            });
    }

    tcp::acceptor acceptor_;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <port>\n";
        return 1;
    }

    // Reap CGI children automatically and survive broken browser pipes.
    std::signal(SIGCHLD, SIG_IGN);
    std::signal(SIGPIPE, SIG_IGN);

    try {
        boost::asio::io_context io;
        HttpServer server(io, static_cast<unsigned short>(std::atoi(argv[1])));
        io.run();
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

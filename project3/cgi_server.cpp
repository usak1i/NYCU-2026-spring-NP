// cgi_server.cpp -> built as cgi_server.exe (Part 2, Windows)
//
// Single-process HTTP server that combines http_server + panel.cgi + console.cgi
// from Part 1. Per spec 2.3:
//   - URIs are guaranteed to be "/panel.cgi" or "/console.cgi?..." only.
//   - No fork/exec/setenv — dispatch inline.
//   - Test cases t1..t5 are hardcoded into the panel form's file menu.
//
// Architecture:
//   - HttpServer:  acceptor loop, one io_context, single-threaded async.
//   - HttpSession: per-connection HTTP request parser. Dispatches by URI:
//       /panel.cgi      -> render panel inline, close.
//       /console.cgi?.. -> hand the socket over to a Console handler that
//                          spawns N Session<SocketEmitter> on the same
//                          io_context. Sessions share the HTTP socket via
//                          shared_ptr; when the last one finishes, the socket
//                          closes and the response is complete.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include "session_runner.hpp"

using boost::asio::ip::tcp;

namespace {

// -------- Emitter that writes to the HTTP client socket ----------
//
// Single-threaded io_context means handlers are serialized, so we can do
// blocking write() here without locks. If the browser disconnected mid-stream
// the write returns an error_code which we silently swallow — the active
// Session<Emit> instances will see the next async_read fail and clean up.

struct SocketEmitter {
    std::shared_ptr<tcp::socket> sock;
    void operator()(const std::string& s) {
        boost::system::error_code ec;
        boost::asio::write(*sock, boost::asio::buffer(s), ec);
        // ignore ec; broken-pipe cleanup happens via Session::finish() on read failure
    }
};

// -------- /console.cgi handler ----------
//
// Holds the HTTP client socket alive until every spawned Session finishes.
// Lifetime: captured by lambdas in each Session's on_done; when the last
// session decrements active->0, the lambda closes the socket; all shared_ptr
// chains then unwind naturally.

void handle_console(boost::asio::io_context& io,
                    std::shared_ptr<tcp::socket> sock,
                    const std::string& query) {
    auto slots = np3::parse_query_string(query);
    std::vector<int> selected;
    for (int i = 0; i < 5; ++i) {
        if (slots[i].selected()) selected.push_back(i);
        else break;  // spec: selected sessions are contiguous from index 0
    }

    // Status line + headers + scaffold all in one synchronous write.
    std::ostringstream resp;
    resp << "HTTP/1.1 200 OK\r\n"
         << "Content-type: text/html\r\n\r\n"
         << np3::build_scaffold_html(selected, slots);
    boost::system::error_code wec;
    boost::asio::write(*sock, boost::asio::buffer(resp.str()), wec);
    if (wec) return;  // browser disappeared before we even started

    if (selected.empty()) {
        boost::system::error_code ec;
        sock->close(ec);
        return;
    }

    auto emitter = std::make_shared<SocketEmitter>(SocketEmitter{sock});
    auto active  = std::make_shared<int>(static_cast<int>(selected.size()));

    auto on_done = [sock, active]() {
        if (--(*active) == 0) {
            boost::system::error_code ec;
            sock->close(ec);
        }
    };

    for (int i : selected) {
        auto sess = std::make_shared<np3::Session<SocketEmitter>>(
            io, i, slots[i].host, slots[i].port,
            "test_case/" + slots[i].file,
            emitter, on_done);
        sess->start();
        // sess goes out of scope, but Session keeps itself alive via
        // shared_from_this() captured in its async handler chain.
    }
}

// -------- /panel.cgi handler ----------

void handle_panel(std::shared_ptr<tcp::socket> sock) {
    const std::string body = np3::build_panel_html();
    std::ostringstream resp;
    resp << "HTTP/1.1 200 OK\r\n"
         << "Content-type: text/html\r\n\r\n"
         << body;
    boost::system::error_code ec;
    boost::asio::write(*sock, boost::asio::buffer(resp.str()), ec);
    sock->close(ec);
}

// -------- HTTP session ----------

class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    HttpSession(boost::asio::io_context& io, tcp::socket socket)
        : io_(io), sock_(std::make_shared<tcp::socket>(std::move(socket))) {}

    void start() {
        auto self = shared_from_this();
        boost::asio::async_read_until(
            *sock_, request_buf_, "\r\n\r\n",
            [this, self](const boost::system::error_code& ec, std::size_t) {
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

        // METHOD URI PROTOCOL
        std::string method, uri;
        size_t s1 = request_line.find(' ');
        if (s1 == std::string::npos) return;
        size_t s2 = request_line.find(' ', s1 + 1);
        if (s2 == std::string::npos) return;
        method = request_line.substr(0, s1);
        uri    = request_line.substr(s1 + 1, s2 - s1 - 1);
        if (method != "GET") return;

        // Drain remaining headers (we don't need any of them for Part 2).
        std::string line;
        while (std::getline(is, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) break;
        }

        // Split URI into path and query.
        std::string path = uri, query;
        size_t q = uri.find('?');
        if (q != std::string::npos) {
            path  = uri.substr(0, q);
            query = uri.substr(q + 1);
        }

        // Spec 2.3: URI is guaranteed to be /panel.cgi or /console.cgi?...
        if (path == "/panel.cgi") {
            handle_panel(sock_);
        } else if (path == "/console.cgi") {
            handle_console(io_, sock_, query);
        } else {
            // Out of spec — just close.
            boost::system::error_code ec;
            sock_->close(ec);
        }
    }

    boost::asio::io_context&      io_;
    std::shared_ptr<tcp::socket>  sock_;
    boost::asio::streambuf        request_buf_;
};

// -------- HTTP server (accept loop) ----------

class HttpServer {
public:
    HttpServer(boost::asio::io_context& io, unsigned short port)
        : io_(io), acceptor_(io, tcp::endpoint(tcp::v4(), port)) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<HttpSession>(io_, std::move(socket))->start();
                }
                do_accept();
            });
    }

    boost::asio::io_context& io_;
    tcp::acceptor            acceptor_;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <port>\n";
        return 1;
    }

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

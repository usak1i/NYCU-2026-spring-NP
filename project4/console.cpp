// console.cpp -> built as console.cgi
//
// CGI program executed by http_server. Responsibilities:
//   1. Parse QUERY_STRING into up to 5 (host, port, file) slots.
//   2. Print the HTTP body header + HTML scaffold (one <pre id="s{i}"> per
//      selected session).
//   3. Run an io_context that drives all selected sessions concurrently.
//      Each session streams chunks back to the browser via injected
//      <script> tags that append to the corresponding <pre>.
//
// http_server has already written "HTTP/1.1 200 OK\r\n" to the socket and
// dup2'd that socket onto our stdout.

#include <array>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include "session_runner.hpp"

namespace {

struct StdoutEmitter {
    void operator()(const std::string& s) {
        std::cout << s;
        std::cout.flush();
    }
};

}  // namespace

int main() {
    // If the browser disconnects mid-stream, EPIPE shouldn't kill us.
    std::signal(SIGPIPE, SIG_IGN);

    const char* qs_env = std::getenv("QUERY_STRING");
    std::string query = qs_env ? qs_env : "";

    auto slots = np3::parse_query_string(query);
    auto socks = np3::parse_socks_target(query);

    // Spec: selected sessions are contiguous from index 0. Stop at first gap.
    std::vector<int> selected;
    for (int i = 0; i < 5; ++i) {
        if (slots[i].selected()) {
            selected.push_back(i);
        } else {
            break;
        }
    }

    // HTTP body header. http_server has already sent the status line.
    std::cout << "Content-type: text/html\r\n\r\n";
    std::cout << np3::build_scaffold_html(selected, slots);
    std::cout.flush();

    if (selected.empty()) {
        // Nothing to do, but still emit a valid response.
        return 0;
    }

    boost::asio::io_context io;
    auto emitter = std::make_shared<StdoutEmitter>();

    int active = static_cast<int>(selected.size());
    auto on_done = [&active]() {
        --active;
        // The io_context will drain naturally when all sessions close.
    };

    std::vector<std::shared_ptr<np3::Session<StdoutEmitter>>> sessions;
    sessions.reserve(selected.size());

    for (int i : selected) {
        auto sess = std::make_shared<np3::Session<StdoutEmitter>>(
            io, i, slots[i].host, slots[i].port,
            "test_case/" + slots[i].file,
            emitter, on_done, socks.host, socks.port);
        sessions.push_back(sess);
        sess->start();
    }

    io.run();
    return 0;
}

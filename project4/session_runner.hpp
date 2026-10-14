// session_runner.hpp
//
// Shared per-session async logic for NP Project 3.
//
// One Session<Emit> handles one connection to a remote shell server (NP Project 2
// style, prompts with "% "). It:
//   - async-resolves and async-connects to (host, port)
//   - reads chunks, renders them via output_shell()
//   - on a "% " prompt boundary, reads the next line of the input file and
//     dispatches it via output_command() + async_write to the remote
//
// The Emit template parameter lets us use the same code for:
//   - Part 1 console.cgi: emit writes to stdout (which http_server has dup2'd
//     onto the HTTP client socket)
//   - Part 2 cgi_server.exe: emit writes to a tcp::socket directly

#ifndef SESSION_RUNNER_HPP
#define SESSION_RUNNER_HPP

#include <array>
#include <boost/system/detail/error_code.hpp>
#include <cstddef>
#include <chrono>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <boost/asio.hpp>

namespace np3 {

// ---------- HTML escaping ----------

// Match Python's html.escape(quote=True). The wire format is a JS literal
// inside a <script>, so any byte that could escape that single-quoted string
// must be neutralized.
inline std::string html_escape(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '&':  out += "&amp;";   break;
            case '<':  out += "&lt;";    break;
            case '>':  out += "&gt;";    break;
            case '"':  out += "&quot;";  break;
            case '\'': out += "&#x27;";  break;
            case '\n': out += "&NewLine;"; break;
            case '\r': /* drop CR; HTML treats LF as line break inside <pre> */ break;
            default:   out += c;         break;
        }
    }
    return out;
}

// ---------- HTML emitters ----------
//
// Both produce a one-line <script> that appends to the per-session <pre>.
// output_command wraps the appended text in <b>...</b> so the user can see
// commands in bold (matches sample_console.cgi).
//
// Uses a free function template so the Emit object can be any callable
// taking a std::string&& (e.g. a lambda capturing stdout / a tcp::socket).

template <class Emit>
inline void output_shell(Emit& emit, int session_idx, const std::string& payload) {
    std::ostringstream oss;
    oss << "<script>document.getElementById('s" << session_idx
        << "').innerHTML += '" << html_escape(payload) << "';</script>\n";
    emit(oss.str());
}

template <class Emit>
inline void output_command(Emit& emit, int session_idx, const std::string& payload) {
    std::ostringstream oss;
    oss << "<script>document.getElementById('s" << session_idx
        << "').innerHTML += '<b>" << html_escape(payload) << "</b>';</script>\n";
    emit(oss.str());
}

// ---------- Session ----------

template <class Emit>
class Session : public std::enable_shared_from_this<Session<Emit>> {
public:
    Session(boost::asio::io_context& io,
            int idx,
            std::string host,
            std::string port,
            std::string file_path,
            std::shared_ptr<Emit> emit,
            std::function<void()> on_done,
            std::string socks_host = {},
            std::string socks_port = {})
        : wait_timer_(io),
          resolver_(io),
          socket_(io),
          idx_(idx),
          host_(std::move(host)),
          port_(std::move(port)),
          socks_host_(std::move(socks_host)),
          socks_port_(std::move(socks_port)),
          file_(file_path),
          emit_(std::move(emit)),
          on_done_(std::move(on_done)) {
              using_socks_ = !socks_host_.empty() && !socks_port_.empty();
          }

    void start() {
        auto self = this->shared_from_this();
        const std::string& rh = using_socks_ ? socks_host_ : host_;
        const std::string& rp = using_socks_ ? socks_port_ : port_;
        resolver_.async_resolve(
            rh, rp,
            [this, self](const boost::system::error_code& ec,
                         boost::asio::ip::tcp::resolver::results_type results) {
                if (ec) { finish(); return; }
                do_connect(std::move(results));
            });
    }

private:
    void do_connect(boost::asio::ip::tcp::resolver::results_type results) {
        auto self = this->shared_from_this();
        boost::asio::async_connect(
            socket_, results,
            [this, self](const boost::system::error_code& ec,
                         const boost::asio::ip::tcp::endpoint&) {
                if (ec) { finish(); return; }
                if (using_socks_) {
                    do_socks_handshake();
                } else {
                    do_read();
                }
            });
    }

    void do_socks_handshake() {
        uint16_t dport = 0;
        try {
            dport = static_cast<uint16_t>(std::stoi(port_));
        } catch (...) {
            finish();
            return;
        }

        // SOCKS 4A CONNECT Request
        auto req = std::make_shared<std::string>();
        req->push_back(char(0x04));                     // VN = 4
        req->push_back(char(0x01));                     // CD = CONNECT
        req->push_back(char((dport >> 8) & 0xff));      // D_PORT hi
        req->push_back(char(dport & 0xff));             // D_PORT lo
        req->push_back(char(0));                        // D_IP byte 0
        req->push_back(char(0));                        // D_IP byte 1
        req->push_back(char(0));                        // D_IP byte 2
        req->push_back(char(1));                        // D_IP byte 3
        req->push_back(char(0));                        // USERID = empty + NULL
        req->append(host_);                             // DEMAIN
        req->push_back(char(0));                        // DEMAIN NULL

        auto self = this->shared_from_this();
        boost::asio::async_write(
            socket_, boost::asio::buffer(*req),
            [this, self, req](const boost::system::error_code& ec, std::size_t) {
                if (ec) {
                    finish();
                    return;
                }
                auto reply = std::make_shared<std::array<unsigned char, 8>>();
                boost::asio::async_read(
                    socket_, boost::asio::buffer(*reply),
                    [this, self, reply](const boost::system::error_code& ec2, std::size_t) {
                        if (ec2 || (*reply)[0] != 0 || (*reply)[1] != 90) {
                            finish();
                            return;
                        }
                        do_read();
                    });
            });
    }

    void do_read() {
        auto self = this->shared_from_this();
        socket_.async_read_some(
            boost::asio::buffer(read_buf_),
            [this, self](const boost::system::error_code& ec, std::size_t n) {
                if (ec) { finish(); return; }
                on_chunk(std::string(read_buf_.data(), n));
                do_read();
            });
    }

    // Render the chunk to the browser, then dispatch one command per "% "
    // seen in the cumulative buffer. The remote shell may bundle a prompt
    // together with subsequent output in a single chunk (numbered-pipe
    // delivery), so suffix-only detection misses prompts. Spec guarantees
    // command output never contains "%", so a substring search is safe.
    void on_chunk(std::string chunk) {
        // 1. Render every byte so the prompt itself appears on the page.
        output_shell(*emit_, idx_, chunk);

        // 2. Append to the pending buffer (carry-over from prior chunks).
        pending_.append(chunk);

        // 3. Drain every "% " in the buffer; each one fires one command.
        const std::string prompt = "% ";
        for (;;) {
            std::size_t at = pending_.find(prompt);
            if (at == std::string::npos) break;
            pending_.erase(0, at + prompt.size());
            dispatch_next_command();
        }


        /*
        std::size_t at = pending_.find(prompt);
        if(at != std::string::npos) {
            pending_.erase(0, at + prompt.size());
            wait_timer_.expires_after(std::chrono::seconds(3));
            auto self = this->shared_from_this();
            wait_timer_.async_wait(
                [this, self](const boost::system::error_code& ec) {
                    if (ec) return;
                    dispatch_next_command();
                });
        }
        */

        // 4. Cap the buffer at 1 KB. We only need to retain enough trailing
        //    bytes to detect a "% " split across the next read.
        if (pending_.size() > kPendingCap) {
            pending_.erase(0, pending_.size() - kPendingCap);
        }
    }

    void dispatch_next_command() {
        std::string line;
        if (!std::getline(file_, line)) {
            // No more commands. Remote shell will handle whatever it has;
            // we just stop sending. Most test cases include "exit" already.
            return;
        }
        // Ensure the line ends with '\n' for the remote shell.
        line.push_back('\n');

        // Render the command in bold for the user.
        output_command(*emit_, idx_, line);

        // Keep the buffer alive until async_write completes.
        auto buf = std::make_shared<std::string>(std::move(line));
        auto self = this->shared_from_this();
        boost::asio::async_write(
            socket_, boost::asio::buffer(*buf),
            [this, self, buf](const boost::system::error_code& ec, std::size_t) {
                if (ec) { finish(); }
                // On success do nothing; do_read() will keep firing.
            });
    }

    void finish() {
        if (finished_) return;
        finished_ = true;
        boost::system::error_code ec;
        wait_timer_.cancel(ec);
        socket_.close(ec);
        if (on_done_) on_done_();
    }

    boost::asio::steady_timer wait_timer_;
    static constexpr std::size_t kPendingCap = 1024;

    boost::asio::ip::tcp::resolver  resolver_;
    boost::asio::ip::tcp::socket    socket_;
    int                             idx_;
    std::string                     host_;
    std::string                     port_;
    std::string                     socks_host_;
    std::string                     socks_port_;
    bool                            using_socks_ = false;
    std::ifstream                   file_;
    std::shared_ptr<Emit>           emit_;
    std::function<void()>           on_done_;
    std::array<char, 4096>          read_buf_{};
    std::string                     pending_;
    bool                            finished_ = false;
};

// ---------- Query string parser ----------
//
// QUERY_STRING is "h0=...&p0=...&f0=...&h1=...&...&f4=". Parse into 5 slots.
// A slot is "selected" iff all three of h{i}, p{i}, f{i} are non-empty.
// Per spec, selected sessions are contiguous from index 0.

struct Slot {
    std::string host;
    std::string port;
    std::string file;
    bool selected() const {
        return !host.empty() && !port.empty() && !file.empty();
    }
};

inline std::string url_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '+') {
            out += ' ';
        } else if (c == '%' && i + 2 < s.size()) {
            auto hex = [](char ch) -> int {
                if (ch >= '0' && ch <= '9') return ch - '0';
                if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
                if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
                return -1;
            };
            int hi = hex(s[i + 1]), lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out += static_cast<char>(hi * 16 + lo);
                i += 2;
            } else {
                out += c;
            }
        } else {
            out += c;
        }
    }
    return out;
}

inline std::array<Slot, 5> parse_query_string(const std::string& qs) {
    std::array<Slot, 5> slots;
    size_t pos = 0;
    while (pos < qs.size()) {
        size_t amp = qs.find('&', pos);
        std::string pair = qs.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        pos = (amp == std::string::npos) ? qs.size() : amp + 1;

        size_t eq = pair.find('=');
        if (eq == std::string::npos) continue;
        std::string key = pair.substr(0, eq);
        std::string val = url_decode(pair.substr(eq + 1));

        if (key.size() != 2) continue;
        char kind = key[0];
        char digit = key[1];
        if (digit < '0' || digit > '4') continue;
        int idx = digit - '0';

        switch (kind) {
            case 'h': slots[idx].host = val; break;
            case 'p': slots[idx].port = val; break;
            case 'f': slots[idx].file = val; break;
            default: break;
        }
    }
    return slots;
}

struct SocksTarget {
    std::string host;
    std::string port;
    bool active() const { return !host.empty() && !port.empty(); }
};

inline SocksTarget parse_socks_target(const std::string& qs) {
    SocksTarget t;
    size_t pos = 0;
    while (pos < qs.size()) {
        size_t amp = qs.find('&', pos);
        std::string pair = qs.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        pos = (amp == std::string::npos) ? qs.size() : amp + 1;

        size_t eq = pair.find('=');
        if (eq == std::string::npos) continue;
        std::string key = pair.substr(0, eq);
        std::string val = url_decode(pair.substr(eq + 1));
        if (key == "sh") {
            t.host = val;
        } else if (key == "sp") {
            t.port = val;
        }
    }
    return t;
}

// ---------- HTML scaffold builders ----------
//
// Shared between Part 1 (console.cpp emits to stdout) and Part 2
// (cgi_server.cpp emits to the HTTP socket directly).

inline std::string build_scaffold_html(const std::vector<int>& selected_indices,
                                       const std::array<Slot, 5>& slots) {
    std::ostringstream html;
    html <<
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "  <head>\n"
        "    <meta charset=\"UTF-8\" />\n"
        "    <title>NP Project 4 Console</title>\n"
        "    <link rel=\"stylesheet\" "
        "href=\"https://cdn.jsdelivr.net/npm/bootstrap@4.5.3/dist/css/bootstrap.min.css\" "
        "integrity=\"sha384-TX8t27EcRE3e/ihU7zmQxVncDAy5uIKz4rEkgIXeMed4M0jlfIDPvg6uqKI2xXr2\" "
        "crossorigin=\"anonymous\" />\n"
        "    <link href=\"https://fonts.googleapis.com/css?family=Source+Code+Pro\" "
        "rel=\"stylesheet\" />\n"
        "    <link rel=\"icon\" type=\"image/png\" "
        "href=\"https://cdn0.iconfinder.com/data/icons/small-n-flat/24/678068-terminal-512.png\" />\n"
        "    <style>\n"
        "      * { font-family: 'Source Code Pro', monospace; font-size: 1rem !important; }\n"
        "      body { background-color: #212529; }\n"
        "      pre { color: #cccccc; }\n"
        "      b { color: #01b468; }\n"
        "    </style>\n"
        "  </head>\n"
        "  <body>\n"
        "    <table class=\"table table-dark table-bordered\">\n"
        "      <thead><tr>";
    for (int i : selected_indices) {
        html << "<th scope=\"col\">"
             << slots[i].host << ":" << slots[i].port
             << "</th>";
    }
    html <<
        "</tr></thead>\n"
        "      <tbody><tr>";
    for (int i : selected_indices) {
        html << "<td><pre id=\"s" << i << "\" class=\"mb-0\"></pre></td>";
    }
    html <<
        "</tr></tbody>\n"
        "    </table>\n"
        "  </body>\n"
        "</html>\n";
    return html.str();
}

// Part 2: render the panel form inline (mirrors the Python panel.cgi but with
// the input-file menu hardcoded to t1.txt..t5.txt per spec section 2.3.2(a)).
// Form action targets "console.cgi" so the submit URL is /console.cgi?...,
// matching Part 1's routing.
inline std::string build_panel_html() {
    constexpr int N_SERVERS = 5;
    std::ostringstream html;
    html <<
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "  <head>\n"
        "    <title>NP Project 4 Panel</title>\n"
        "    <link rel=\"stylesheet\" "
        "href=\"https://cdn.jsdelivr.net/npm/bootstrap@4.5.3/dist/css/bootstrap.min.css\" "
        "integrity=\"sha384-TX8t27EcRE3e/ihU7zmQxVncDAy5uIKz4rEkgIXeMed4M0jlfIDPvg6uqKI2xXr2\" "
        "crossorigin=\"anonymous\" />\n"
        "    <link href=\"https://fonts.googleapis.com/css?family=Source+Code+Pro\" "
        "rel=\"stylesheet\" />\n"
        "    <link rel=\"icon\" type=\"image/png\" "
        "href=\"https://cdn4.iconfinder.com/data/icons/iconsimple-setting-time/512/dashboard-512.png\" />\n"
        "    <style>* { font-family: 'Source Code Pro', monospace; }</style>\n"
        "  </head>\n"
        "  <body class=\"bg-secondary pt-5\">\n"
        "    <form action=\"console.cgi\" method=\"GET\">\n"
        "      <table class=\"table mx-auto bg-light\" style=\"width: inherit\">\n"
        "        <thead class=\"thead-dark\">\n"
        "          <tr>\n"
        "            <th scope=\"col\">#</th>\n"
        "            <th scope=\"col\">Host</th>\n"
        "            <th scope=\"col\">Port</th>\n"
        "            <th scope=\"col\">Input File</th>\n"
        "          </tr>\n"
        "        </thead>\n"
        "        <tbody>\n";

    // Build host menu (12 nplinux machines) and file menu (t1..t5).
    std::string host_menu;
    for (int h = 1; h <= 12; ++h) {
        host_menu += "<option value=\"nplinux" + std::to_string(h) +
                     ".cs.nycu.edu.tw\">nplinux" + std::to_string(h) + "</option>";
    }
    std::string file_menu;
    for (int t = 1; t <= 5; ++t) {
        std::string name = "t" + std::to_string(t) + ".txt";
        file_menu += "<option value=\"" + name + "\">" + name + "</option>";
    }

    for (int i = 0; i < N_SERVERS; ++i) {
        html <<
            "          <tr>\n"
            "            <th scope=\"row\" class=\"align-middle\">Session " << (i + 1) << "</th>\n"
            "            <td>\n"
            "              <div class=\"input-group\">\n"
            "                <select name=\"h" << i << "\" class=\"custom-select\">\n"
            "                  <option></option>" << host_menu << "\n"
            "                </select>\n"
            "                <div class=\"input-group-append\">\n"
            "                  <span class=\"input-group-text\">.cs.nycu.edu.tw</span>\n"
            "                </div>\n"
            "              </div>\n"
            "            </td>\n"
            "            <td>\n"
            "              <input name=\"p" << i << "\" type=\"text\" class=\"form-control\" size=\"5\" />\n"
            "            </td>\n"
            "            <td>\n"
            "              <select name=\"f" << i << "\" class=\"custom-select\">\n"
            "                <option></option>" << file_menu << "\n"
            "              </select>\n"
            "            </td>\n"
            "          </tr>\n";
    }

    html <<
        "          <tr>\n"
        "            <td colspan=\"3\"></td>\n"
        "            <td>\n"
        "              <button type=\"submit\" class=\"btn btn-info btn-block\">Run</button>\n"
        "            </td>\n"
        "          </tr>\n"
        "        </tbody>\n"
        "      </table>\n"
        "    </form>\n"
        "  </body>\n"
        "</html>\n";
    return html.str();
}

}  // namespace np3

#endif  // SESSION_RUNNER_HPP

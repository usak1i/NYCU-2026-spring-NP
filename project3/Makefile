CXX      = g++
# Modern Boost (>=1.66) keeps Asio header-only; BOOST_ERROR_CODE_HEADER_ONLY
# avoids the link dep on libboost_system, which Homebrew no longer ships.
CXXFLAGS = -Wall -O2 -std=c++14 -pthread -DBOOST_ERROR_CODE_HEADER_ONLY
LDLIBS   = -pthread

# If BOOST_ROOT is unset, try to auto-detect a non-system Boost install.
# This makes plain `make` work on macOS (Homebrew puts Boost outside the
# default search paths) without forcing the user to remember `BOOST_ROOT=...`.
# On NP Linux these wildcards miss and we fall through to /usr/include/boost.
ifeq ($(BOOST_ROOT),)
  ifneq ($(wildcard /opt/homebrew/include/boost/asio.hpp),)
    BOOST_ROOT := /opt/homebrew
  else ifneq ($(wildcard /usr/local/include/boost/asio.hpp),)
    BOOST_ROOT := /usr/local
  endif
endif

ifneq ($(BOOST_ROOT),)
  CXXFLAGS += -I$(BOOST_ROOT)/include
  LDFLAGS  += -L$(BOOST_ROOT)/lib
endif

.PHONY: all part1 part2 clean stage_cgis
all: part1 part2

# === Part 1 (Linux): http_server + console.cgi ===
# These are the only C++ binaries the demo expects from `make part1`.
# TA-provided Python CGIs (printenv.cgi, panel.cgi) live in working_dir/,
# not next to source — so they are NOT a dependency of part1.
part1: http_server console.cgi

# === Part 2 (Windows): single executable cgi_server.exe ===
# On Linux/macOS we still build it (named with .exe for spec compliance).
part2: cgi_server.exe

# === C++ binaries ===

http_server: http_server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

console.cgi: console.cpp session_runner.hpp
	$(CXX) $(CXXFLAGS) -o $@ console.cpp $(LDFLAGS) $(LDLIBS)

cgi_server.exe: cgi_server.cpp session_runner.hpp
	$(CXX) $(CXXFLAGS) -o $@ cgi_server.cpp $(LDFLAGS) $(LDLIBS)

# === Local dev convenience: chmod TA-provided CGIs in cwd ===
# Run only when those files happen to be in the working directory
# (e.g. during local testing outside the demo flow).
stage_cgis:
	@for f in printenv.cgi panel.cgi; do \
	  if [ -f $$f ]; then chmod +x $$f && echo "[ok] $$f ready"; fi; \
	done

clean:
	rm -f http_server console.cgi cgi_server.exe

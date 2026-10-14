CXX      = g++
CXXFLAGS = -Wall -O2 -std=c++14 -pthread -DBOOST_ERROR_CODE_HEADER_ONLY
LDLIBS   = -pthread

.PHONY: all clean
all: socks_server pj4.cgi

socks_server: socks_server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDLIBS)

pj4.cgi: console.cpp session_runner.hpp
	$(CXX) $(CXXFLAGS) -o $@ console.cpp $(LDLIBS)

clean:
	rm -f socks_server pj4.cgi

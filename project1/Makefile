CXX = g++
CXXFLAGS = -Wall -O2 -pedantic -std=c++11

.PHONY: all clean

all: npshell np_simple

npshell: npshell.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

np_simple: np_simple.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f npshell np_simple

CXX = g++
CXXFLAGS = -Wall -O2 -std=c++11

.PHONY: all clean

all: np_single_proc np_multi_proc

np_single_proc: np_single_proc.cpp
	$(CXX) $(CXXFLAGS) -o np_single_proc np_single_proc.cpp

np_multi_proc: np_multi_proc.cpp
	$(CXX) $(CXXFLAGS) -o np_multi_proc np_multi_proc.cpp

clean:
	rm -f np_single_proc np_multi_proc

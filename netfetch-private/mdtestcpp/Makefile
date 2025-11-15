#/*****************************************************************************\
#*                                                                             *
#*       Copyright (c) 2003, The Regents of the University of California       *
#*     See the file COPYRIGHT for a complete copyright notice and license.     *
#*                                                                             *
#******************************************************************************/
#
# Purpose:
#       Make mdtest and dumptrace executables.
#       make        -- build all (mdtest and dumptrace)
#       make clean  -- remove executables and object files
#

include ../common/mk/recipes.mk

# Compiler selection based on OS
OS = $(shell uname)
CC.Linux = g++ -w
CC.Darwin = g++ -w
CC = $(CC.$(OS))
CXX = g++

# HdrHistogram setup
HDR_HISTOGRAM_DIR = third_party/HdrHistogram_c
HDR_HISTOGRAM_INSTALL = $(HDR_HISTOGRAM_DIR)/install
HDR_HISTOGRAM_SRC = $(wildcard $(HDR_HISTOGRAM_DIR)/src/*.c)
HDR_HISTOGRAM_OBJS = $(patsubst %.c, %.o, $(HDR_HISTOGRAM_SRC))
# Compiler flags -DNETFETCH_DEBUG
CFLAGS = -O3 -g -pthread -lm -ltbb -lcrypto -lboost_system -lboost_thread -lpthread -Wno-deprecated-declarations -fPIE -std=c++17 #-DNETFETCH_DEBUG
# LARGE_FILE_FLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE=1 -D__USE_LARGEFILE64=1 
CFLAGS += $(LARGE_FILE_FLAGS) $(MDTEST_FLAGS)
CFLAGS += -I$(HDR_HISTOGRAM_INSTALL)/include
LDFLAGS += -lm -L$(HDR_HISTOGRAM_INSTALL)/lib -lhdr_histogram_static
# LDFLAGS += $(HDR_HISTOGRAM_SRC)
# Add glib flags
# GLIB_CFLAGS = $(shell pkg-config --cflags glib-2.0)
# GLIB_LIBS = $(shell pkg-config --libs glib-2.0)
# CFLAGS += $(GLIB_CFLAGS)
# LDFLAGS += $(GLIB_LIBS)
# Source files
MDTEST_SRCS = mdtest.cpp iniparser.c dictionary.c hashring.cpp fssocket.cpp wrapper.cpp common_impl.cpp threadpool.cpp ticker.cpp workload.cpp
MDTEST_RATE_SRCS = mdtest_rate.cpp iniparser.c dictionary.c hashring.cpp fssocket.cpp wrapper.cpp common_impl.cpp threadpool.cpp ticker.cpp workload_rate.cpp

# MDTEST_LATENCY_SRCS = mdtest_latency.cpp iniparser.c dictionary.c hashring.cpp fssocket.cpp wrapper.cpp common_impl.cpp threadpool.cpp ticker.cpp workload.cpp
DUMPTRACE_SRCS = dumptrace_fast.cpp iniparser.c dictionary.c common_impl.cpp
DUMPTRACE_RM_SRCS = dumptrace_fast_rmdir.cpp iniparser.c dictionary.c common_impl.cpp
REQUESTS_SRCS = gen_requests.cpp 
TEST_SRCS = test.cpp
# Object files
MDTEST_OBJS = $(filter %.o, $(patsubst %.cpp, %.o, $(MDTEST_SRCS))) $(filter %.o, $(patsubst %.c, %.o, $(DUMPTRACE_SRCS)))
MDTEST_RATE_OBJS = $(filter %.o, $(patsubst %.cpp, %.o, $(MDTEST_RATE_SRCS))) $(filter %.o, $(patsubst %.c, %.o, $(DUMPTRACE_SRCS)))

# MDTEST_LATENCY_OBJS = $(filter %.o, $(patsubst %.cpp, %.o, $(MDTEST_LATENCY_SRCS))) $(filter %.o, $(patsubst %.c, %.o, $(DUMPTRACE_SRCS)))
DUMPTRACE_OBJS = $(filter %.o, $(patsubst %.cpp, %.o, $(DUMPTRACE_SRCS))) $(filter %.o, $(patsubst %.c, %.o, $(DUMPTRACE_SRCS)))
DUMPTRACE_RM_OBJS = $(filter %.o, $(patsubst %.cpp, %.o, $(DUMPTRACE_RM_SRCS))) $(filter %.o, $(patsubst %.c, %.o, $(DUMPTRACE_RM_SRCS)))
REQUESTS_OBJS = $(REQUESTS_SRCS:.cpp=.o)
TEST_OBJS = $(TEST_SRCS:.cpp=.o)
# Targets
.PHONY: all clean

# all: mdtest mdtest_latency dumptrace_fast dumptrace_fast_rmdir #gen_requests
all: mdtest mdtest_rate dumptrace_fast dumptrace_fast_rmdir #gen_requests

%.o: %.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@



# Build test program with hdr_histogram
# test: $(TEST_OBJS) 
# 	$(CXX) -o $@ $(TEST_OBJS) $(CFLAGS) $(LDFLAGS)

# mdtest build
# mdtest_latency: $(MDTEST_LATENCY_OBJS)
# 	$(CXX) -o $@ $(MDTEST_LATENCY_OBJS) $(CFLAGS) $(LDFLAGS)

mdtest: $(MDTEST_OBJS)
	$(CXX) -o $@ $(MDTEST_OBJS) $(CFLAGS) $(LDFLAGS)
mdtest_rate: $(MDTEST_RATE_OBJS)
	$(CXX) -o $@ $(MDTEST_RATE_OBJS) $(CFLAGS) $(LDFLAGS)

# dumptrace build
dumptrace_fast: $(DUMPTRACE_OBJS)
	$(CXX) -o $@ $(DUMPTRACE_OBJS) -lcrypto

dumptrace_fast_rmdir: $(DUMPTRACE_RM_OBJS)
	$(CXX) -o $@ $(DUMPTRACE_RM_OBJS) -lcrypto

gen_requests: $(REQUESTS_OBJS)
	$(CXX) -o $@ $(REQUESTS_OBJS) -lcrypto
# Cleanup
clean:
	# echo $(MDTEST_OBJS)
	# echo $(DUMPTRACE_OBJS)
	# echo $(REQUESTS_OBJS)
	rm -f mdtest dumptrace dumptrace_fast gen_requests $(MDTEST_OBJS) $(DUMPTRACE_OBJS) $(REQUESTS_OBJS) 

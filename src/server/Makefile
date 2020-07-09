
MODE = DB_TEST
#LOCK = USE_ILOCK
#LOCK = USE_ROW_LOCK
LOCK = USE_LOCK_TABLE
HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)

BRPC_INCLUDE = ../../third_party/brpc/output/include
BRPC_LIB = ../../third_party/brpc/output/lib
CO_INCLUDE =  ../../third_party/libco
CO_LIB = ../../third_party/libco/build

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf` -g -Wno-address-of-packed-member -Wno-deprecated-register -Wno-format
CXXFLAGS += -std=c++11 -I.. -D${MODE} -D${LOCK} -I../../third_party/mintomic/include -I${BRPC_INCLUDE} -I${CO_INCLUDE} -O2
KBUILD_CFLAGS += $(call cc-disable-warning, address-of-packed-member)

ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf`\
           -ldl
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf`\
           -Wl,--no-as-needed -Wl,--as-needed\
           -ldl
endif

LDFLAGS += ../common/libcommon.a -L${BRPC_LIB} -L${CO_LIB}
LIBS =  -lpthread -lm -ltcmalloc  -lbrpc -lcolib

all: DbServer

objects = db_server.o  \
					main.o \
					packet.o \
					db_impl.o \
					db_worker.o \
					tpcc_load.o \
					trans_mgr.o \
					lock_mgr.o  \
					lock_table.o \
					dep_graph.o \
					ilock.o

storage = DbImpl.o \
					main.o \


DbServer: ${objects} 
	$(CXX) $^ $(LDFLAGS) $(LIBS) -o $@

store : ${storage}
	$(CXX) $^ $(LIBS) -o $@

clean:
	rm -f *.o *.pb.cc *.pb.h  DbServer

# iTrans
An interactive main-memory database prototype with efficient transaction processing. 

# compile gperf

git clone https://github.com/gperftools/gperftools.git

cd gperftools

./autogen.sh

./configure

make -j10

sudo make install

# compile leveldb

cd leveldb

make -j10

# compile gflags

cd gflags

cmake . -DBUILD_SHARED_LIBS=1 -DBUILD_STATIC_LIBS=1

make -j10

sudo make install

# compile protobuf

cd protobuf

./autogen.sh

./configure

make -j10

sudo make install

# compile brpc

sh config_brpc.sh --headers=/usr/local/include --libs=/usr/local/lib

make -j10

# How to build and run iTrans

make -j10

cd server

./Dbserver [args]

cd ../client

./TpccClinet [args]

#!/bin/bash
rm -rf /dev/shm/rocksdb.*
rm -rf /dev/shm/pmdk_cache
mkdir /dev/shm/pmdk_cache
# USE_PMDK=1 DISABLE_WARNING_AS_ERROR=1 ROCKSDB_NO_FBCODE=1 ROCKSDB_DISABLE_MALLOC_USABLE_SIZE=1 DEBUG_LEVEL=2 make -j16 pmdk_cache_test
# gdb --args ./pmdk_cache_test --gtest_catch_exceptions=0
USE_PMDK=1 DISABLE_WARNING_AS_ERROR=1 ROCKSDB_NO_FBCODE=1 ROCKSDB_DISABLE_MALLOC_USABLE_SIZE=1 DEBUG_LEVEL=2 make -j16 cache_bench
gdb --args ./cache_bench -use_pmdk_cache=true -cache_size=8388608 #-threads=1
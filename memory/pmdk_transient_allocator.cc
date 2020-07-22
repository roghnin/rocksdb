//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#define PMDK
#ifdef PMDK

#include "pmdk_transient_allocator.h"
#include <iostream>
#include <libpmemobj.h>
// TODO: make the following macros external arguments
#define SHM_SIMULATE
#ifdef SHM_SIMULATE
#define HEAP_FILE "/dev/shm/pmem/pmdk_transient"
#else
#define HEAP_FILE "/mnt/pmem/pmdk_transient"
#endif

#define PMEM_MAX_SIZE 1024*1024*32 // 32MiB

namespace rocksdb {

// TODO: use c++ bindings in this example.
PMDKTransAllocator::PMDKTransAllocator(){
  pop = pmemobj_create(HEAP_FILE, "test", PMEM_MAX_SIZE, 0666);
  if (!pop){
      throw std::bad_alloc();
  }
  else {
      root = pmemobj_root(pop, sizeof(void*));
  }
}

void* PMDKTransAllocator::Allocate(size_t size) {
  PMEMoid temp_ptr;
  int ret = pmemobj_alloc(pop, &temp_ptr, size, 0, dummy_construct, nullptr);
  if (ret == -1){
      throw std::bad_alloc();
  } else {
      return pmemobj_direct(temp_ptr);
  }
}

void PMDKTransAllocator::Deallocate(void* p) {
  if (!p){
      return;
  }
  PMEMoid temp_ptr;
  temp_ptr = pmemobj_oid(p);
  pmemobj_free(&temp_ptr);
}

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
size_t PMDKTransAllocator::UsableSize(void* p,
                                        size_t /*allocation_size*/) const {
  return pmemobj_alloc_usable_size(pmemobj_oid(p));
}
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

}  // namespace rocksdb
#endif  // PMDK

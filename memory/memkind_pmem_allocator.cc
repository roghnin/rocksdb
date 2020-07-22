//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef MEMKIND

#include "memkind_pmem_allocator.h"
#include <iostream>
// TODO: make the following macros external arguments
#define SHM_SIMULATE
#ifdef SHM_SIMULATE
#define PMEM_DIR "/dev/shm/pmem/memkind"
#else
#define PMEM_DIR "/mnt/pmem/memkind"
#endif

#define PMEM_MAX_SIZE 1024*1024*256 // 256MiB

namespace rocksdb {

MemkindPmemAllocator::MemkindPmemAllocator(){
  assert(PMEM_MAX_SIZE > MEMKIND_PMEM_MIN_SIZE);
  auto err = memkind_create_pmem(PMEM_DIR, PMEM_MAX_SIZE, &pmem_kind);
  if (err != 0) {
    std::cerr<<"memkind_create_pmem failed with error "<<err<<std::endl;
    std::cerr<<"MEMKIND_PMEM_MIN_SIZE: "<<MEMKIND_PMEM_MIN_SIZE<<std::endl;

  }
}

void* MemkindPmemAllocator::Allocate(size_t size) {
  void* p = memkind_malloc(pmem_kind, size);
  if (p == NULL) {
    throw std::bad_alloc();
  }
  return p;
}

void MemkindPmemAllocator::Deallocate(void* p) {
  memkind_free(pmem_kind, p);
}

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
size_t MemkindPmemAllocator::UsableSize(void* p,
                                        size_t /*allocation_size*/) const {
  return memkind_malloc_usable_size(pmem_kind, p);
}
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

}  // namespace rocksdb
#endif  // MEMKIND

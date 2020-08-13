//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/nvm_cache.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "util/mutexlock.h"

#define PHEAP_PATH "/dev/shm/nvm_cache"



namespace ROCKSDB_NAMESPACE {

NVMCacheShard::NVMCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy)
    : capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex) {
  set_metadata_charge_policy(metadata_charge_policy);
  // TODO

  // Set up persistent memory pool (pop)
  if (access(PHEAP_PATH, F_OK) != 0){
    pop_ = po::pool<PersistentRoot>::create(PHEAP_PATH, "nvm_cache_pool", PMEMOBJ_MIN_POOL, S_IRWXU);
    PersistentRoot* root = pop_.root().get();
    po::transaction::run(pop_, [&, root] {
      root->persistent_hashmap = po::make_persistent<PersistTierHashTable>();
      persistent_hashmap_ = root->persistent_hashmap.get();
    });
    
  } else {
    pop_ = po::pool<PersistentRoot>::open(PHEAP_PATH, "nvm_cache_pool");
    persistent_hashmap_ = pop_.root()->persistent_hashmap.get();
  }

  // get hashmap from root.
  

}

void NVMCacheShard::EraseUnRefEntries() {
  // TODO
}

void NVMCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                           bool thread_safe) {
  // TODO
}

void NVMCacheShard::TEST_GetLRUList(TransientHandle** lru, TransientHandle** lru_low_pri) {
  MutexLock l(&mutex_);
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
}

size_t NVMCacheShard::TEST_GetLRUSize() {
  size_t lru_size = 0;
  // TODO
  return lru_size;
}

double NVMCacheShard::GetHighPriPoolRatio() {
  MutexLock l(&mutex_);
  return high_pri_pool_ratio_;
}

void NVMCacheShard::LRU_Remove(TransientHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(lru_usage_ >= total_charge);
  lru_usage_ -= total_charge;
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void NVMCacheShard::LRU_Insert(TransientHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += total_charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of LRU list.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    lru_low_pri_ = e;
  }
  lru_usage_ += total_charge;
}

void NVMCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    assert(lru_low_pri_ != &lru_);
    lru_low_pri_->SetInHighPriPool(false);
    size_t total_charge =
        lru_low_pri_->CalcTotalCharge(metadata_charge_policy_);
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void NVMCacheShard::EvictFromLRU(size_t charge,
                                 autovector<TransientHandle*>* deleted) {
  // TODO
}

void NVMCacheShard::SetCapacity(size_t capacity) {
  autovector<TransientHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromLRU(0, &last_reference_list);
  }

  // Free the entries outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void NVMCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Cache::Handle* NVMCacheShard::Lookup(const Slice& key, uint32_t hash) {
  // TODO
  return nullptr;
}

bool NVMCacheShard::Ref(Cache::Handle* h) {
  TransientHandle* e = reinterpret_cast<TransientHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void NVMCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  MutexLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool NVMCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  // TODO
  return true;
}

Status NVMCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value),
                             Cache::Handle** handle, Cache::Priority priority) {
  Status s = Status::OK();
  
  // insertion into persistent tier
  {
    MutexLock l(&mutex_);

    // TODO: evict from LRU
    // TODO: calculate charge and refuse insert if cache is full
    po::transaction::run(pop_, [&]{
      auto p_entry = po::make_persistent<PersistentEntry>();
      p_entry->key_size = key.size();
      p_entry->key = po::make_persistent<char[]>(key.size());
      pop_.memcpy_persist(p_entry->key.get(), key.data(), key.size());
      persistent_hashmap_->insert(PersistTierHashTable::value_type(hash,
        po::make_persistent<PersistentEntry>()));
    });
  }
  
  return s;
}

void NVMCacheShard::Erase(const Slice& key, uint32_t hash) {
  // TODO
}

size_t NVMCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t NVMCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

std::string NVMCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

NVMCache::NVMCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<NVMCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(NVMCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        NVMCacheShard(per_shard, strict_capacity_limit, high_pri_pool_ratio,
                      use_adaptive_mutex, metadata_charge_policy);
  }
}

NVMCache::~NVMCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~NVMCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* NVMCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* NVMCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* NVMCache::Value(Handle* handle) {
  return reinterpret_cast<const TransientHandle*>(handle)->value;
}

size_t NVMCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const TransientHandle*>(handle)->charge;
}

uint32_t NVMCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const TransientHandle*>(handle)->hash;
}

void NVMCache::DisownData() {
// Do not drop data if compile with ASAN to suppress leak warning.
#if defined(__clang__)
#if !defined(__has_feature) || !__has_feature(address_sanitizer)
  shards_ = nullptr;
  num_shards_ = 0;
#endif
#else  // __clang__
#ifndef __SANITIZE_ADDRESS__
  shards_ = nullptr;
  num_shards_ = 0;
#endif  // !__SANITIZE_ADDRESS__
#endif  // __clang__
}

size_t NVMCache::TEST_GetLRUSize() {
  size_t lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lru_size_of_all_shards += shards_[i].TEST_GetLRUSize();
  }
  return lru_size_of_all_shards;
}

double NVMCache::GetHighPriPoolRatio() {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

std::shared_ptr<Cache> NewNVMCache(const LRUCacheOptions& cache_opts) {
  return NewNVMCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
                     cache_opts.metadata_charge_policy);
}

std::shared_ptr<Cache> NewNVMCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<NVMCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      std::move(memory_allocator), use_adaptive_mutex, metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE

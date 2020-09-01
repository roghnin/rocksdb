//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/pmdk_cache.h"

#include <experimental/filesystem>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>


#include "util/mutexlock.h"

// TODO: make these run-time variables
#define PHEAP_PATH "/dev/shm/pmdk_cache/"
#define PMEMOBJ_POOL_SIZE (size_t)(1024*1024*1024)



namespace ROCKSDB_NAMESPACE {

PMDKCacheShard::PMDKCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy,
                             size_t shard_id)
    : capacity_(0),
      // TODO: set persistent_capacity dynamically.
      persistent_capacity_(PMEMOBJ_POOL_SIZE),
      strict_capacity_limit_(strict_capacity_limit),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex) {
  set_metadata_charge_policy(metadata_charge_policy);

  // TODO: set up an LRUCacheShard as transient tier.

  // Set up persistent memory pool (pop)
  // TODO: use an alternative to access() that works on all platforms.
  std::string heap_file = PHEAP_PATH + std::to_string(shard_id);
  if (access(heap_file.c_str(), F_OK) != 0){
    // TODO: name pool with shard id.
    pop_ = po::pool<PersistentRoot>::create(heap_file, "pmdk_cache_pool", PMEMOBJ_POOL_SIZE, S_IRWXU);
    PersistentRoot* root = pop_.root().get();
    po::transaction::run(pop_, [&, root] {
      root->persistent_hashtable = po::make_persistent<PersistTierHashTable>(&pop_);
      root->persistent_lru_list = po::make_persistent<PersistentEntry>();
      root->era = 0;
      persistent_hashtable_ = root->persistent_hashtable.get();
      lru_ = root->persistent_lru_list;
    });
    era_ = 0;
  } else {
    pop_ = po::pool<PersistentRoot>::open(heap_file, "pmdk_cache_pool");
    persistent_hashtable_ = pop_.root()->persistent_hashtable.get();
    persistent_hashtable_->SetPop(&pop_);
    lru_ = pop_.root()->persistent_lru_list;
    era_ = ++pop_.root()->era;
  }
}

void PMDKCacheShard::EraseUnRefEntries() {
  // TODO: call transient.
  po::transaction::run(pop_, [&] {
    autovector<po::persistent_ptr<PersistentEntry>> last_reference_list;
    {
      MutexLock l(&mutex_);
      while(lru_->next_lru != lru_) {
        po::persistent_ptr<PersistentEntry> old = lru_->next_lru;
        // LRU list contains only elements which can be evicted
        assert(old->InCache() && !old->HasRefs());
        LRU_Remove(old);
        persistent_hashtable_->Remove(old);
        old->SetInCache(false);
        size_t total_charge = old->persist_charge;
        assert(usage_ >= total_charge);
        usage_ -= total_charge;
        last_reference_list.push_back(old);
      }
    }
    for (auto entry : last_reference_list) {
      FreePEntry(entry);
    }
  });
}

void PMDKCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                           bool thread_safe) {
  // TODO
}

void PMDKCacheShard::TEST_GetLRUList(TransientHandle** lru, TransientHandle** lru_low_pri) {
  // MutexLock l(&mutex_);
  // *lru = &lru_;
  // *lru_low_pri = lru_low_pri_;
  // TODO
}

size_t PMDKCacheShard::TEST_GetLRUSize() {
  size_t lru_size = 0;
  // TODO
  return lru_size;
}

void PMDKCacheShard::LRU_Remove(po::persistent_ptr<PersistentEntry> e) {
  assert(e->next_lru != nullptr);
  assert(e->prev_lru != nullptr);
  po::transaction::run(pop_, [&, e] {
    e->next_lru->prev_lru = e->prev_lru;
    e->prev_lru->next_lru = e->next_lru;
    e->prev_lru = e->next_lru = nullptr;
  });
  size_t persist_charge = e->persist_charge;
  assert(lru_usage_ >= persist_charge);
  lru_usage_ -= persist_charge;
}

void PMDKCacheShard::LRU_Insert(po::persistent_ptr<PersistentEntry> e) {
  assert(e->next_lru == nullptr);
  assert(e->prev_lru == nullptr);
  po::transaction::run(pop_, [&, e] {
    // Inset "e" to head of LRU list.
    e->next_lru = lru_;
    e->prev_lru = lru_->prev_lru;
    e->prev_lru->next_lru = e;
    e->next_lru->prev_lru = e;
  });
  lru_usage_ += e->persist_charge;
}

void PMDKCacheShard::EvictFromLRU(size_t charge,
                                 autovector<po::persistent_ptr<PersistentEntry>>* deleted) {
  while ((usage_ + charge) > persistent_capacity_ && lru_->next_lru != lru_) {
    po::persistent_ptr<PersistentEntry> old = lru_->next_lru;
    // LRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    LRU_Remove(old);
    persistent_hashtable_->Remove(old);
    old->SetInCache(false);
    size_t old_persist_charge = old->persist_charge;
    assert(usage_ >= old_persist_charge);
    usage_ -= old_persist_charge;
    deleted->push_back(old);
  }
}

TransientHandle* PMDKCacheShard::GetTransientHandle(po::persistent_ptr<PersistentEntry> e,
                                            void* (*pack)(const Slice& slice),
                                            void (*deleter)(const Slice& key, void* value)){
  if (e == nullptr){
    return nullptr;
  }
  // TODO: this transaction might not be necessary if the underlying NVM has word-level
  // store atomicity.
  po::transaction::run(pop_, [&, e] {
    if (e->era < era_){
      e->era = era_;
      e->trans_handle = nullptr;
    }
  });
  TransientHandle* ret = e->trans_handle;
  if (!ret){
    // build a TransientHandle.
    ret = new TransientHandle();
    ret->key_data = e->key.get();
    ret->key_length = e->key_size;
    ret->value = pack(Slice(e->val.get(), e->val_size));
    ret->p_entry = e;
    ret->deleter = deleter;
    e->trans_handle = ret; // no need to be in transaction. It's transient.
  }
  return ret;
}

void PMDKCacheShard::FreePEntry(po::persistent_ptr<PersistentEntry> e){
  // This must be called within a transaction, so no transaction needed here.
  // free key, val, trans_handle, the transient "coat" (Block or BlockContent) of val,
  // and persistent entry.

  // free transient handle first, since the deleter may use e->key and/or e->value.
  if (e->era == era_ && e->trans_handle != nullptr){
    (*e->trans_handle->deleter)(Slice(e->val.get(), e->val_size), e->trans_handle->value);
    delete e->trans_handle;
  }
  po::delete_persistent<char[]>(e->key, (int)e->key_size);
  po::delete_persistent<char[]>(e->val, (int)e->val_size);
  po::delete_persistent<PersistentEntry>(e);
}

bool PMDKCacheShard::IsLRUHandle(Cache::Handle* e){
  HandleClassifier* hc = reinterpret_cast<HandleClassifier*>(e);
  if (hc->type == reinterpret_cast<void*>(0x1)){
    return false;
  } else {
    return true;
  }
}

void PMDKCacheShard::SetCapacity(size_t capacity) {
  // TODO: call transient SetCapacity.
}

void PMDKCacheShard::SetPersistentCapacity(size_t capacity) {
  po::transaction::run(pop_, [&, capacity] {
    autovector<po::persistent_ptr<PersistentEntry>> last_reference_list;
    {
      MutexLock l(&mutex_);
      persistent_capacity_ = capacity;
      EvictFromLRU(0, &last_reference_list);
    }

    // Free the entries outside of mutex for performance reasons
    for (auto entry : last_reference_list) {
      FreePEntry(entry);
    }
  });
}

void PMDKCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit){
  // TODO: call transient SetStrictCapacityLimit().
}

Cache::Handle* PMDKCacheShard::Lookup(const Slice& key, uint32_t hash,
                                      void* (*pack)(const Slice& slice),
                                      void (*deleter)(const Slice&, void* value)) {
  
  MutexLock l(&mutex_);
  // TODO: lookup transient hash table

  // lookup persistent table:
  po::persistent_ptr<PersistentEntry> e = persistent_hashtable_->Lookup(key, hash);
  TransientHandle* th = nullptr;
  if (e != nullptr){
    th = GetTransientHandle(e, pack, deleter);
    assert(e->InCache());
    if (!e->HasRefs()){
      LRU_Remove(e);
    }
    th->Ref();
  }
  return reinterpret_cast<Cache::Handle*>(th);
}

bool PMDKCacheShard::Ref(Cache::Handle* h) {
  // TODO: transient tier Ref.
  TransientHandle* e = reinterpret_cast<TransientHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

// void PMDKCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
//   // TODO: call transient.
// }

bool PMDKCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr){
    return false;
  }
  bool last_reference = false;
  if (IsLRUHandle(handle)){
    // TODO: release from transient tier

    if (force_erase){
      // TODO: erase in persistent tier
      // grab the key in LRUHandle, find the persistent entry, and remove it.
    }
    // return false in this case, since
    // there must be an entry in persistent tier.
    last_reference = false;
  } else {
    bool* last_reference_p = &last_reference;
    po::transaction::run(pop_, [&, handle, force_erase, last_reference_p] {
      TransientHandle* e = reinterpret_cast<TransientHandle*>(handle);
      {
        MutexLock l(&mutex_);
        *last_reference_p = e->Unref();
        if (*last_reference_p && e->p_entry->InCache()) {
          // The item is still in cache, and nobody else holds a reference to it
          if (usage_ > persistent_capacity_ || force_erase){
            // The LRU list must be empty since the cache is full
            assert(lru_->next_lru == lru_ || force_erase);
            // Take this opportunity and remove the item
            persistent_hashtable_->Remove(e->p_entry);
            e->p_entry->SetInCache(false);
          } else {
            // Put the item back on the LRU list, and don't free it
            LRU_Insert(e->p_entry);
            *last_reference_p = false;
          }
        }
        if (*last_reference_p){
          size_t total_charge = e->p_entry->persist_charge;
          assert(usage_ >= total_charge);
          usage_ -= total_charge;
        }
      }
      if (*last_reference_p){
        // TODO: double-check if this is able to free
        // both persistent and transient metadata.
        FreePEntry(e->p_entry);
      }
    });
  }
  return last_reference;
}

Status PMDKCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value),
                             Cache::Handle** handle, Cache::Priority priority,
                             const Slice (*unpack)(void* packed),
                             void* (*pack)(const Slice& value)) {

  Status s;
  Status* s_p = &s;
  autovector<po::persistent_ptr<PersistentEntry>> last_reference_list;

  // insert into persistent tier first, since failed insertion into transient tier
  // may end up deleting value.

  if (unpack != nullptr){
    assert(deleter != nullptr && pack != nullptr);
    const Slice& unpacked_val = unpack(value);
    // TODO: better estimation of persistent total charge:
    size_t persistent_charge = sizeof(PersistentEntry) + 
                                key.size() + 
                                unpacked_val.size() + 
                                // rough estimation of hash map entry: cache line size,
                                // since each hash map node requires its mutex padded
                                // to cache line.
                                _SC_LEVEL2_CACHE_LINESIZE;

    po::transaction::run(pop_, [&, unpacked_val, persistent_charge, s_p, deleter, pack] {
      {
        MutexLock l(&mutex_);
        EvictFromLRU(persistent_charge, &last_reference_list);
        // TODO: calculate charge and refuse insert if cache is full
        if ((usage_ + persistent_charge) > persistent_capacity_){
          if (handle != nullptr) {
            *handle = nullptr;
            *s_p = Status::Incomplete("Insert failed due to LRU cache being full.");
          }
          // else, do nothing here and pretend the insertion succeeded but entry got
          // kicked out immediately.
        } else {
          // create new persistent entry
          auto p_entry = po::make_persistent<PersistentEntry>();
          p_entry->persist_charge = persistent_charge;
          p_entry->key_size = key.size();
          p_entry->key = po::make_persistent<char[]>(key.size());
          p_entry->val_size = unpacked_val.size();
          p_entry->val = po::make_persistent<char[]>(unpacked_val.size());
          p_entry->hash = hash;
          // memcpy from transient to persistent tier
          pop_.memcpy_persist(p_entry->key.get(), key.data(), key.size());
          pop_.memcpy_persist(p_entry->val.get(), unpacked_val.data(), unpacked_val.size());
          // insert persistent entry into hash table.
          po::persistent_ptr<PersistentEntry> old = persistent_hashtable_->Insert(hash, key, p_entry);
          usage_ += persistent_charge;
          if (old != nullptr){
            *s_p = Status::OkOverwritten();
            assert(old->InCache());
            old->SetInCache(false);
            if (!old->HasRefs()){
              LRU_Remove(old);
              size_t old_persist_charge = old->persist_charge;
              assert(usage_ >= old_persist_charge);
              usage_ -= old_persist_charge;
              last_reference_list.push_back(old);
            }
          }
          if (handle == nullptr){
            LRU_Insert(p_entry);
          } else {
            // TODO: don't do this when insertion into transient tier
            // is successful. Always insert into LRU instead.
            TransientHandle* e = GetTransientHandle(p_entry, pack, deleter);
            e->Ref();
            *handle = reinterpret_cast<Cache::Handle*>(e);
          }
        }
      }
      // Free the entries here outside of mutex for performance reasons
      for (auto entry : last_reference_list) {
        FreePEntry(entry);
      }
    });

    if (s != Status::OK()){
      if (deleter){
        (*deleter)(key, value);
      }
      return s;
    }
  }


  
  // TODO: insert into transient tier.

  return s;
}

void PMDKCacheShard::Erase(const Slice& key, uint32_t hash) {
  // TODO: erase from transient tier

  // erase from persistent tier
  po::transaction::run(pop_, [&, key, hash] {
    bool last_reference;
    po::persistent_ptr<PersistentEntry> e;
    {
      MutexLock l(&mutex_);
      e = persistent_hashtable_->Remove(key, hash);
      if (e != nullptr) {
        assert(e->InCache());
        e->SetInCache(false);
        if (!e->HasRefs()){
          // The entry is in LRU since it's in hash and has no external references
          LRU_Remove(e);
          size_t total_charge = e->persist_charge;
          assert(usage_ >= total_charge);
          usage_ -= total_charge;
          last_reference = true;
        }
      }
    }
    if (last_reference) {
      FreePEntry(e);
    }
  });
}

size_t PMDKCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t PMDKCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

std::string PMDKCacheShard::GetPrintableOptions() const {
  // TODO
}

PMDKCache::PMDKCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<PMDKCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(PMDKCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  // TODO: create directory `PHEAP_PATH` here.
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        PMDKCacheShard(per_shard, strict_capacity_limit, high_pri_pool_ratio,
                      use_adaptive_mutex, metadata_charge_policy, (size_t)i);
  }
}

PMDKCache::~PMDKCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~PMDKCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* PMDKCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* PMDKCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* PMDKCache::Value(Handle* handle) {
  return reinterpret_cast<const TransientHandle*>(handle)->value;
}

size_t PMDKCache::GetCharge(Handle* handle) const {
  // return reinterpret_cast<const TransientHandle*>(handle)->charge;
  // TODO
  return 0;
}

uint32_t PMDKCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const TransientHandle*>(handle)->hash;
}

void PMDKCache::DisownData() {
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

size_t PMDKCache::TEST_GetLRUSize() {
  size_t lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lru_size_of_all_shards += shards_[i].TEST_GetLRUSize();
  }
  return lru_size_of_all_shards;
}

std::shared_ptr<Cache> NewPMDKCache(const LRUCacheOptions& cache_opts) {
  return NewPMDKCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
                     cache_opts.metadata_charge_policy);
}

std::shared_ptr<Cache> NewPMDKCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  // if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
  //   // invalid high_pri_pool_ratio
  //   return nullptr;
  // }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<PMDKCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      std::move(memory_allocator), use_adaptive_mutex, metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE

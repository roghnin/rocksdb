//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <string>

#include "cache/sharded_cache.h"

#include "port/malloc.h"
#include "port/port.h"
#include "util/autovector.h"

// TODO: re-order included headers.
#include <unistd.h>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/transaction.hpp>

namespace po = pmem::obj;

namespace ROCKSDB_NAMESPACE {

// This will be the transient handle of the cache.
// TODO: maybe we can have fixed-size keys, as they're always built from persistent entry?
// or, maybe we don't even need a key field in the handle.
struct TransientHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  TransientHandle* next_hash;
  TransientHandle* next;
  TransientHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  void Free() {
    assert(refs == 0);
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }

  // Caclculate the memory usage by metadata
  inline size_t CalcTotalCharge(
      CacheMetadataChargePolicy metadata_charge_policy) {
    size_t meta_charge = 0;
    if (metadata_charge_policy == kFullChargeCacheMetadata) {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      meta_charge += malloc_usable_size(static_cast<void*>(this));
#else
      // This is the size that is used when a new handle is created
      meta_charge += sizeof(TransientHandle) - 1 + key_length;
#endif
    }
    return charge + meta_charge;
  }
};

struct PersistentEntry{
  // persistent fields:
  po::p<size_t> key_size;
  po::p<size_t> val_size;
  po::p<size_t> persist_charge;
  po::persistent_ptr<char[]> key;
  po::persistent_ptr<char[]> val;
  po::persistent_ptr<PersistentEntry> next_hash = nullptr;
  po::persistent_ptr<PersistentEntry> next_lru = nullptr;
  po::persistent_ptr<PersistentEntry> prev_lru = nullptr;
  // persistent flags:
  po::p<bool> in_cache;

  // TODO: set era to be current era.
  size_t era = 0;
  // transient fields, validated with era number:
  TransientHandle* trans_handle = nullptr;

  void Free(){
    // TODO:
    // free key, val, trans_handle, the transient "coat" (Block or BlockContent) of val, and this.
  }
  void Ref(){
    assert(trans_handle);
    trans_handle->Ref();
  }
  bool InCache(){
    return in_cache;
  }
  bool HasRefs(){
    if (trans_handle == nullptr){
      return false;
    } else {
      return trans_handle->HasRefs();
    }
  }
  void SetInCache(bool x){
    in_cache = x;
  }
};

using PHashTableType = po::concurrent_hash_map<po::p<uint32_t>, po::persistent_ptr<PersistentEntry>>;
class PersistTierHashTable{
  po::persistent_ptr<PHashTableType> table_;
  bool KeyEqual(const char* data, size_t size, po::persistent_ptr<PersistentEntry> entry){
    if (size != entry->key_size){
      return false;
    }
    return (memcmp(data, entry->key.get(), size) == 0);
  }
  po::persistent_ptr<PersistentEntry>* FindPointer(const Slice& key, uint32_t hash){
    po::persistent_ptr<PersistentEntry>* ptr;
    PHashTableType::accessor acc;
    bool res = table_->find(acc, hash);
    if (res){
      ptr = &acc->second;
      while(*ptr != nullptr && (!KeyEqual(key.data(), key.size(), (*ptr)))) {
        ptr = &(*ptr)->next_hash;
      }
    } else {
      table_->insert(acc, PHashTableType::value_type(hash, nullptr));
      ptr = &acc->second;
    }
    return ptr;
  }
public:
  PersistTierHashTable(po::persistent_ptr<PHashTableType> table) : table_(table) {}

  po::persistent_ptr<PersistentEntry> Insert(
      uint32_t hash, const Slice& key, po::persistent_ptr<PersistentEntry> entry){
    po::persistent_ptr<PersistentEntry>* ptr = FindPointer(key, hash);
    po::persistent_ptr<PersistentEntry> old = *ptr;
    entry->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = entry;
    return old;
  }
  po::persistent_ptr<PersistentEntry> Lookup(const Slice& key, uint32_t hash){
    return (*FindPointer(key, hash));
  }
  
  po::persistent_ptr<PersistentEntry> Remove(const Slice& key, uint32_t hash){
    // TODO
    // this may only be used by Erase().
  }
  po::persistent_ptr<PersistentEntry> Remove(po::persistent_ptr<PersistentEntry> e){
    // TODO
    // this is used by EvictFromLRU(). consider having hash persisted in PersistentEntry.
  }
};

struct PersistentRoot{
  po::persistent_ptr<PHashTableType> persistent_hashtable;
  po::persistent_ptr<PersistentEntry> persistent_lru_list;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) PMDKCacheShard final : public CacheShard {
 public:
  PMDKCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy);
  virtual ~PMDKCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of PMDKCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;
  void SetPersistentCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle,
                        Cache::Priority priority,
                        const Slice& (*unpack)(void* value) = nullptr,
                        void* (*pack)(const Slice& value) = nullptr) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                                 void* (*pack)(const Slice& value) = nullptr) override;
  virtual bool Ref(Cache::Handle* handle) override;
  virtual bool Release(Cache::Handle* handle,
                       bool force_erase = false) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(TransientHandle** lru, TransientHandle** lru_low_pri);

  //  Retrieves number of elements in LRU, for unit test purpose only
  //  not threadsafe
  size_t TEST_GetLRUSize();

  //  Retrives high pri pool ratio
  double GetHighPriPoolRatio();

 private:
  void LRU_Remove(po::persistent_ptr<PersistentEntry> e);
  void LRU_Insert(po::persistent_ptr<PersistentEntry> e);

  TransientHandle* GetTransientHandle(po::persistent_ptr<PersistentEntry> e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, autovector<po::persistent_ptr<PersistentEntry>>* deleted);

  // Initialized before use.
  size_t capacity_;
  size_t persistent_capacity_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Dummy head of persistent LRU list.
  // lru->prev_lru is newest entry, lru->next_lru is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  po::persistent_ptr<PersistentEntry> lru_;

  // Pointer to head of low-pri pool in LRU list.
  TransientHandle* lru_low_pri_;

  po::pool<PersistentRoot> pop_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  // TransientHandleTable table_;

  // This is a concurrent persistent container provided by PMDK.
  PersistTierHashTable* persistent_hashtable_;

  // TODO: recover memory usage and lru usage data after crash, or consider persisting them.

  // Memory size for entries residing in the cache
  size_t usage_;

  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;
};

class PMDKCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  PMDKCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio,
           std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
           bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
           CacheMetadataChargePolicy metadata_charge_policy =
               kDontChargeCacheMetadata);
  virtual ~PMDKCache();
  virtual const char* Name() const override { return "PMDKCache"; }
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;

  //  Retrieves number of elements in LRU, for unit test purpose only
  size_t TEST_GetLRUSize();
  //  Retrives high pri pool ratio
  double GetHighPriPoolRatio();

 private:
  PMDKCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE

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

struct HandleClassifier{
  void* foo;
  void* type;
};

struct PersistentEntry;

// This will be the transient handle of the cache.
struct TransientHandle {
  void* value;
  // a fixed number to tell TransientHandle and LRUHandle apart.
  void* type = reinterpret_cast<void*>(0x1);
  // deleter of *value
  void (*deleter)(const Slice& key, void* value);
  // length of key on persistent memory.
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;
  // a pointer to persistent entry
  po::persistent_ptr<PersistentEntry> p_entry;
  // pointer to a key in persistent memory
  char* key_data;

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
};

struct PersistentEntry{
  // persistent fields:
  po::p<size_t> key_size;
  po::p<size_t> val_size;
  po::p<size_t> persist_charge;
  po::persistent_ptr<char[]> key;
  po::persistent_ptr<char[]> val;
  po::p<uint32_t> hash;
  po::persistent_ptr<PersistentEntry> next_hash = nullptr;
  po::persistent_ptr<PersistentEntry> next_lru = nullptr;
  po::persistent_ptr<PersistentEntry> prev_lru = nullptr;
  // persistent flags:
  po::p<bool> in_cache;

  // TODO: set era to be current era.
  size_t era = 0;
  // transient fields, validated with era number:
  TransientHandle* trans_handle = nullptr;

  // void Ref(){
  //   assert(trans_handle);
  //   trans_handle->Ref();
  // }
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

class PersistentRoot;

class PersistTierHashTable{
  po::persistent_ptr<po::persistent_ptr<PersistentEntry>[]> list_;
  po::p<uint32_t> length_;
  po::p<uint32_t> elems_;

  // pop_ is transient. it gets reset during pmdk cache's construction.
  // TODO: protect this with era number. (maybe not necessary.)
  po::pool<PersistentRoot>* pop_;

  bool KeyEqual(const char* data, size_t size, po::persistent_ptr<PersistentEntry> entry){
    if (size != entry->key_size){
      return false;
    }
    return (memcmp(data, entry->key.get(), size) == 0);
  }
  po::persistent_ptr<PersistentEntry>* FindPointer(const Slice& key, uint32_t hash){
    po::persistent_ptr<PersistentEntry>* ptr = &list_[hash & (length_-1)];
    while((*ptr) != nullptr && ((*ptr)->hash != hash || !KeyEqual(key.data(), key.size(), (*ptr)))) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize(){
    uint32_t new_length = 16;
    while(new_length < elems_ * 1.5){
      new_length *= 2;
    }
    po::persistent_ptr<po::persistent_ptr<PersistentEntry>[]> new_list = 
      po::make_persistent<po::persistent_ptr<PersistentEntry>[]>(new_length);
    pop_->memset_persist(new_list.get(), 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++){
      po::persistent_ptr<PersistentEntry> h = list_[i];
      while(h.get() != nullptr){
        po::persistent_ptr<PersistentEntry> next = h->next_hash;
        uint32_t hash = h->hash;
        po::persistent_ptr<PersistentEntry>* ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    po::delete_persistent<po::persistent_ptr<PersistentEntry>[]>(list_, length_);
    list_ = new_list;
    length_ = new_length;
  }
public:
  PersistTierHashTable(po::pool<PersistentRoot>* pop) :
    list_(nullptr), length_(0), elems_(0), pop_(pop) {
    Resize();
  }

  void SetPop(po::pool<PersistentRoot>* pop){
    // TODO: set/update era number
    pop_ = pop;
  }

  po::persistent_ptr<PersistentEntry> Insert(
      uint32_t hash, const Slice& key, po::persistent_ptr<PersistentEntry> entry){
    po::persistent_ptr<PersistentEntry>* ptr = FindPointer(key, hash);
    po::persistent_ptr<PersistentEntry> old = *ptr;
    entry->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = entry;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_){
        Resize();
      }
    }
    return old;
  }

  po::persistent_ptr<PersistentEntry> Lookup(const Slice& key, uint32_t hash){
    return (*FindPointer(key, hash));
  }
  
  // this may only be used by Erase().
  po::persistent_ptr<PersistentEntry> Remove(const Slice& key, uint32_t hash){
    po::persistent_ptr<PersistentEntry>* ptr = FindPointer(key, hash);
    po::persistent_ptr<PersistentEntry> result = *ptr;
    if (result != nullptr){
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

  // this is used by EvictFromLRU().
  po::persistent_ptr<PersistentEntry> Remove(po::persistent_ptr<PersistentEntry> e){
    return Remove(Slice(e->key.get(), e->key_size), e->hash);
  }
};

struct PersistentRoot{
  po::persistent_ptr<PersistTierHashTable> persistent_hashtable;
  po::persistent_ptr<PersistentEntry> persistent_lru_list;
  po::p<size_t> era;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) PMDKCacheShard final : public CacheShard {
 public:
  PMDKCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy,
                size_t shard_id);
  ~PMDKCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of PMDKCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  void SetCapacity(size_t capacity) override;
  void SetPersistentCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle,
                        Cache::Priority priority,
                        const Slice (*unpack)(void* value) = nullptr,
                        void* (*pack)(const Slice& value) = nullptr) override;
  Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                                 void* (*pack)(const Slice& value) = nullptr,
                                 void (*deleter)(const Slice&, void* value) = nullptr) override;
  bool Ref(Cache::Handle* handle) override;
  bool Release(Cache::Handle* handle,
                       bool force_erase = false) override;
  void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  size_t GetUsage() const override;
  size_t GetPinnedUsage() const override;

  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  void EraseUnRefEntries() override;

  std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(TransientHandle** lru, TransientHandle** lru_low_pri);

  //  Retrieves number of elements in LRU, for unit test purpose only
  //  not threadsafe
  size_t TEST_GetLRUSize();

 private:
  void LRU_Remove(po::persistent_ptr<PersistentEntry> e);
  void LRU_Insert(po::persistent_ptr<PersistentEntry> e);

  TransientHandle* GetTransientHandle(po::persistent_ptr<PersistentEntry> e,
                                      void* (*pack)(const Slice& slice),
                                      void (*deleter)(const Slice& key, void* value));

  void FreePEntry(po::persistent_ptr<PersistentEntry> e);

  // Tell apart LRUHandle and TransientHandle by looking at the second word.
  // The second word of LRUHandle is function pointer, either null or an address
  // The second word of TransientHandle will always be 0x1.
  bool IsLRUHandle(Cache::Handle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, autovector<po::persistent_ptr<PersistentEntry>>* deleted);

  // Initialized before use.
  size_t capacity_;
  size_t persistent_capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Dummy head of persistent LRU list.
  // lru->prev_lru is newest entry, lru->next_lru is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  po::persistent_ptr<PersistentEntry> lru_;

  // This is a concurrent persistent container provided by PMDK.
  // TODO: make this an instance rather than a pointer.
  po::persistent_ptr<PersistTierHashTable> persistent_hashtable_;

  // Current era number. Advanced every crash.
  size_t era_;

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

  // PMDK's persistent memory pool;
  po::pool<PersistentRoot> pop_;

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
  const char* Name() const override { return "PMDKCache"; }
  CacheShard* GetShard(int shard) override;
  const CacheShard* GetShard(int shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  void DisownData() override;

  //  Retrieves number of elements in LRU, for unit test purpose only
  size_t TEST_GetLRUSize();

 private:
  PMDKCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE

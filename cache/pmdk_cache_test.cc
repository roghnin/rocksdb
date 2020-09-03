//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/pmdk_cache.h"

#include <string>
#include <vector>
#include "port/port.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class PMDKCacheTest : public testing::Test {
 struct Value{
   Slice slice;
   Value(const std::string& s): slice(s.c_str(), s.size()+1){}
   Value(const Slice& value): slice(value){}
 };

 public:
  PMDKCacheTest() {}
  ~PMDKCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~PMDKCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  static void ValueDeleter(const Slice& /*key*/ ,void* value){
    delete reinterpret_cast<Value*>(value);
  }

  static const Slice ValueUnpack(void* packed){
    return reinterpret_cast<Value*>(packed)->slice;
  }

  static void* ValuePack(const Slice& value){
    return new Value(value);
  }

  void NewCache(size_t capacity, size_t persist_capacity, double high_pri_pool_ratio = 0.0,
                bool use_adaptive_mutex = kDefaultToAdaptiveMutex) {
    DeleteCache();
    cache_ = reinterpret_cast<PMDKCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(PMDKCacheShard)));
    new (cache_) PMDKCacheShard(capacity, false /*strict_capcity_limit*/,
                               high_pri_pool_ratio, use_adaptive_mutex,
                               kDontChargeCacheMetadata, persist_capacity, (size_t)0);
  }

  void Insert(const std::string& key) {
    cache_->Insert(key, 0 /*hash*/, new Value(key), 1 /*charge*/,
                    &ValueDeleter, nullptr /*handle*/, Cache::Priority::LOW,
                    &ValueUnpack, &ValuePack);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/, &ValuePack, &ValueDeleter);
    if (handle) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys) {
    PersistentEntry* lru;
    cache_->TEST_GetLRUList(&lru);
    PersistentEntry* iter = lru;
    for (const auto& key : keys){
      iter = iter->next_lru.get();
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, std::string(iter->val.get()));
    }
    ASSERT_EQ(lru, iter->next_lru.get());
  }

 private:
  PMDKCacheShard* cache_ = nullptr;
};

TEST_F(PMDKCacheTest, BasicLRU) {
  size_t entry_charge = PMDKCache::GetBasePersistCharge() +
    sizeof(char) /*key*/ + sizeof(char)+1 /*val*/;
  NewCache(5, 5*entry_charge);
  // prepare payloads:
  std::string payloads1[] = {"a", "b", "c", "d", "e"};
  for (int i = 0; i < 5; i++){
    Insert(payloads1[i]);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"});

  std::string payloads2[] = {"x", "y", "z"};
  for (int i = 0; i < 3; i++) {
    Insert(payloads2[i]);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"});
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"});
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"});
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"});
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"});
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"});

  // try recovery
  NewCache(5, 5*entry_charge);
  ValidateLRUList({"e", "z", "d", "u", "v"});
  ASSERT_TRUE(Lookup("e"));
  ASSERT_TRUE(Lookup("z"));
  ASSERT_TRUE(Lookup("d"));
  ASSERT_TRUE(Lookup("u"));
  ASSERT_TRUE(Lookup("v"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

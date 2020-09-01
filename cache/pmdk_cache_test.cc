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
   Value(const std::string& s): slice(s){}
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

  void NewCache(size_t capacity, double high_pri_pool_ratio = 0.0,
                bool use_adaptive_mutex = kDefaultToAdaptiveMutex) {
    DeleteCache();
    cache_ = reinterpret_cast<PMDKCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(PMDKCacheShard)));
    new (cache_) PMDKCacheShard(capacity, false /*strict_capcity_limit*/,
                               high_pri_pool_ratio, use_adaptive_mutex,
                               kDontChargeCacheMetadata, (size_t)0);
  }

  void Insert(const std::string& key) {
    cache_->Insert(key, 0 /*hash*/, new Value(key), 1 /*charge*/,
                    &ValueDeleter, nullptr /*handle*/, Cache::Priority::LOW,
                    &ValueUnpack, &ValuePack);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/);
    if (handle) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0) {
    
  }

 private:
  PMDKCacheShard* cache_ = nullptr;
};

TEST_F(PMDKCacheTest, BasicLRU) {
  NewCache(5);
  // prepare payloads:
  std::string payloads[] = {"a", "b", "c", "d", "e"};
  for (int i = 0; i < 5; i++){
    Insert(payloads[i]);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"});
  // for (char ch = 'x'; ch <= 'z'; ch++) {
  //   Insert(ch);
  // }
  // ValidateLRUList({"d", "e", "x", "y", "z"});
  // ASSERT_FALSE(Lookup("b"));
  // ValidateLRUList({"d", "e", "x", "y", "z"});
  // ASSERT_TRUE(Lookup("e"));
  // ValidateLRUList({"d", "x", "y", "z", "e"});
  // ASSERT_TRUE(Lookup("z"));
  // ValidateLRUList({"d", "x", "y", "e", "z"});
  // Erase("x");
  // ValidateLRUList({"d", "y", "e", "z"});
  // ASSERT_TRUE(Lookup("d"));
  // ValidateLRUList({"y", "e", "z", "d"});
  // Insert("u");
  // ValidateLRUList({"y", "e", "z", "d", "u"});
  // Insert("v");
  // ValidateLRUList({"e", "z", "d", "u", "v"});
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

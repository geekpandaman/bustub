//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
// GDB entry:
//bustub::HashTableTest_SampleTest_Test::TestBody
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  int sampleSize = 4000;
  // insert a few values
  for (int i = 0; i < sampleSize; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < sampleSize; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < sampleSize; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  for(int i = 0;i!=sampleSize;i++){
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if(i==0)
      EXPECT_EQ(1,res.size());
    else
      EXPECT_EQ(2,res.size());
  }

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, sampleSize+5, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < sampleSize; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < sampleSize; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}
//bustub::HashTableTest_SplitTest_Test::TestBody
TEST(HashTableTest, SplitTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  size_t sampleSize = 4000;
  for (size_t i = 0; i < sampleSize;i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }
  ht.VerifyIntegrity();

  for(size_t i = 0;i!=sampleSize;i++){
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(res.size(),1);
    EXPECT_EQ(res[0],i);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}
//bustub::HashTableTest_MergeTest_Test::TestBody
TEST(HashTableTest, MergeTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  size_t sampleSize = 4000;
  for (size_t i = 0; i < sampleSize;i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }
  ht.VerifyIntegrity();
  for (size_t i = 0; i < sampleSize;i++) {
    bool success = ht.Remove(nullptr, i, i);
    if(!success){
      LOG_ERROR("Remove %d:%d failed",(int)i,(int)i);
    }
    if(i%100==0)
      ht.VerifyIntegrity();
  }
  ht.VerifyIntegrity();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // 初始化目录
  Page* new_page = buffer_pool_manager_->NewPage(&directory_page_id_,nullptr);
  HashTableDirectoryPage* dir_page = new (new_page->GetData())HashTableDirectoryPage;
  dir_page->SetPageId(directory_page_id_);
  //初始化第一个桶数组
  page_id_t buc_page_id;
  new_page = buffer_pool_manager_->NewPage(&buc_page_id,nullptr);
  HASH_TABLE_BUCKET_TYPE* buc_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*> (new_page->GetData());
  buc_page->Init();
  dir_page->SetBucketPageId(0,buc_page_id);

  //结束后unpin
  buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,true,nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key)&dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  //这里有一个隐式类型转换
  //也就是一定会返回有效的page_id
  return dir_page->GetBucketPageId(Hash(key)&dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage*>(buffer_pool_manager_->FetchPage(directory_page_id_,nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id,nullptr)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  auto buc_page_id = KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);

  bool success = buc_page->GetValue(key,comparator_,result);

  buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,false,nullptr);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  auto buc_page_id = KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);

  bool success = buc_page->Insert(key,value,comparator_);

  buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,success,nullptr);
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  auto buc_page_id = KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);
  //满了就分裂，不判断是否存在重复
  //可能会存在多次分裂也分裂不出空间的情况，因此需要循环分裂
  while(buc_page->IsFull()){
    //split
    uint32_t buc_idx = KeyToDirectoryIndex(key,dir_page);
    if(dir_page->GetLocalDepth(buc_idx)==dir_page->GetGlobalDepth()){
      dir_page->IncrGlobalDepth();
    }
    uint32_t split_mask = dir_page->GetLocalHighBit(buc_idx);
    uint32_t split_buc_idx = buc_idx|(1<<split_mask);
    page_id_t split_buc_page_id = INVALID_PAGE_ID;
    Page* split_page = buffer_pool_manager_->NewPage(&split_buc_page_id,nullptr);
    dir_page->SetBucketPageId(split_buc_idx,split_buc_page_id);
    //分类所有KV
    HASH_TABLE_BUCKET_TYPE* split_buc_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(split_page->GetData());
    split_buc_page->Init();
    for(size_t i = 0;i!=BUCKET_ARRAY_SIZE;i++){
      assert(buc_page->IsReadable(i));
      auto idx_key = buc_page->KeyAt(i);
      auto idx_value = buc_page->ValueAt(i);
      if((Hash(idx_key)&split_mask)!=0){
        //需要放到分裂桶中的
        //这里直接调insert是一个O(n)的算法，可以优化
        split_buc_page->Insert(idx_key,idx_value,comparator_);
        buc_page->RemoveAt(i);
      }
    }
    //设置local_depth++
    dir_page->IncrLocalDepth(buc_idx);
    dir_page->IncrLocalDepth(split_buc_idx);
    //进行unpin，取新的页面判断是否可以插入
    buffer_pool_manager_->UnpinPage(buc_page_id,true,nullptr);
    buffer_pool_manager_->UnpinPage(split_buc_page_id,true,nullptr);
    auto buc_page_id = KeyToPageId(key,dir_page);
    buc_page = FetchBucketPage(buc_page_id);
  }
  //进行插入，若失败说明存在重复
  bool success = buc_page->Insert(key,value,comparator_);
  //unpin循环中没有unpin的页面
  buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,true,nullptr);
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  auto buc_page_id = KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);
  bool success = buc_page->Remove(key,value,comparator_);
  if(success&&buc_page->IsEmpty()){
    buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
    buffer_pool_manager_->UnpinPage(buc_page_id,success,nullptr);
    Merge(transaction,key,value);
    return success;
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,success,nullptr);
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  auto buc_page_id = KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);
  assert(buc_page->IsEmpty());
  auto buc_page_idx = KeyToDirectoryIndex(key,dir_page);
  uint32_t merge_mask = (dir_page->GetLocalHighBit(buc_page_idx)>>1);
  if(merge_mask==0){
    buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
    buffer_pool_manager_->UnpinPage(buc_page_id,true,nullptr);
    return;
  }
  //fetch merge image
  //warning:不对mergepage进行操作会不会有问题
  auto merge_page_idx = buc_page_idx^merge_mask;
  auto merge_page_id = dir_page->GetBucketPageId(merge_page_idx);
  dir_page->SetBucketPageId(buc_page_idx,merge_page_id);
  dir_page->DecrLocalDepth(buc_page_idx);
  dir_page->DecrLocalDepth(merge_page_idx);
  buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
  buffer_pool_manager_->UnpinPage(buc_page_id,false,nullptr);
  buffer_pool_manager_->DeletePage(buc_page_id,nullptr);

}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

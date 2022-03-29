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
  //  allocate a directory page and a bucket page
  Page *page = buffer_pool_manager->NewPage(&directory_page_id_);
  page_id_t bucket_id;
  assert(page != nullptr);
  HashTableDirectoryPage *directory = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  directory->SetPageId(directory_page_id_);
  page = buffer_pool_manager->NewPage(&bucket_id);
  assert(page != nullptr);
  directory->SetBucketPageId(0, bucket_id);
  buffer_pool_manager->UnpinPage(bucket_id, true);
  buffer_pool_manager->UnpinPage(directory_page_id_, true);
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
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetBucketData(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_id = KeyToPageId(key, directory_page);
  Page *page = FetchBucketPage(bucket_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketData(page);
  bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  Page *page = FetchBucketPage(bucket_page_id);
  bool success;
  bool full = false;
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketData(page);
  success = bucket_page->Insert(key, value, comparator_);
  if (!success) {
    /* fail may because of duplicated key,value */
    full = bucket_page->IsFull();
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  if (full) {
    return SplitInsert(transaction, key, value);
  }
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  uint32_t ind0 = KeyToDirectoryIndex(key, directory_page);
  uint32_t ind1 = ind0 ^ directory_page->GetLocalHighBit(ind0);
  Page *page = FetchBucketPage(bucket_page_id);
  bool directory_dirty = false;
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketData(page);
  while (!bucket_page->Insert(key, value, comparator_)) {
    directory_dirty = true;
    /* increase global depth if needed*/
    if (directory_page->GetLocalDepth(ind0) == directory_page->GetGlobalDepth()) {
      size_t oldsize = directory_page->Size();
      if (oldsize >= 512) {
        goto fail;
      }
      directory_page->IncrGlobalDepth();
      for (size_t i = oldsize; i < directory_page->Size(); ++i) {
        directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(i - oldsize));
        directory_page->SetBucketPageId(i, directory_page->GetBucketPageId(i - oldsize));
      }
    }
    ind0 = KeyToDirectoryIndex(key, directory_page);
    ind1 = ind0 ^ directory_page->GetLocalHighBit(ind0);
    /* increase local depth */
    page_id_t new_page_id;
    Page *new_page;
    HASH_TABLE_BUCKET_TYPE *new_bucket;
    assert(directory_page->GetBucketPageId(ind0) == directory_page->GetBucketPageId(ind1));
    assert(directory_page->GetLocalDepth(ind0) == directory_page->GetLocalDepth(ind1));
    directory_page->IncrLocalDepth(ind0);
    directory_page->IncrLocalDepth(ind1);
    new_page = buffer_pool_manager_->NewPage(&new_page_id);
    directory_page->SetBucketPageId(ind1, new_page_id);
    assert(new_page != nullptr);
    new_page->WLatch();
    new_bucket = GetBucketData(new_page);
    for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
      if ((Hash(bucket_page->KeyAt(i)) & directory_page->GetLocalDepthMask(ind0)) == ind1) {
        new_bucket->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_);
        bucket_page->RemoveAt(i);
      }
    }
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_page_id, true);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, directory_dirty);
  table_latch_.WUnlock();
  directory_page->PrintDirectory();
  return true;

fail:
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, directory_dirty);
  table_latch_.WUnlock();
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  Page *page = FetchBucketPage(bucket_page_id);
  bool success;
  bool empty = false;
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketData(page);
  success = bucket_page->Remove(key, value, comparator_);
  empty = bucket_page->IsEmpty();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  if (!success) {
    return false;
  }
  if (empty) {
    Merge(transaction, key, value);
  }
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketData(page);
  if (bucket_page->IsEmpty()) {
    uint32_t ind0 = KeyToDirectoryIndex(key, directory_page);
    uint32_t local_depth = directory_page->GetLocalDepth(ind0);
    uint32_t ind1 = ind0 ^ (1 << (local_depth - 1));
    if (local_depth > 0 && local_depth == directory_page->GetLocalDepth(ind1)) {
      /* need merge */
      directory_page->DecrLocalDepth(ind0);
      directory_page->DecrLocalDepth(ind1);
      directory_page->SetBucketPageId(ind0, directory_page->GetBucketPageId(ind1));
      if (directory_page->CanShrink()) {
        directory_page->DecrGlobalDepth();
      }
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      buffer_pool_manager_->DeletePage(bucket_page_id);
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      table_latch_.WUnlock();
      return;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.WUnlock();
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

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_id;
  std::lock_guard<std::mutex> guard(latch_);
  ValidatePageId(page_id);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id = iter->second;
    pages_[frame_id].RLatch();
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(page_id, pages_[frame_id].data_); 
    }
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].RUnlatch();
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  frame_id_t frame_id;
  std::lock_guard<std::mutex> guard(latch_);
  for (auto iter : page_table_) {
    frame_id = iter.second;
    pages_[frame_id].RLatch();
    if (pages_->IsDirty()) {
      disk_manager_->WritePage(iter.first, pages_[frame_id].GetData());
      pages_->is_dirty_ = false;
    }
    pages_[frame_id].RUnlatch();
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  std::lock_guard<std::mutex> guard(latch_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    goto found;
  }
  if (replacer_->Victim(&frame_id)) {
    page_id_t victim_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(victim_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(victim_page_id);
    goto found;
  }
  *page_id = INVALID_PAGE_ID;
  return nullptr;
found:
  page_id_t pid = AllocatePage();
  Page *page = pages_ + frame_id;
  page_table_[pid] = frame_id;
  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = pid;
  page->pin_count_ = 1;
  *page_id = pid;
  replacer_->Pin(frame_id);
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  ValidatePageId(page_id);
  frame_id_t frame_id;
  std::lock_guard<std::mutex> guard(latch_);
  ValidatePageId(page_id);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id = iter->second;
    goto found;
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    pages_[frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    goto found;
  }
  if (replacer_->Victim(&frame_id)) {
    page_id_t victim_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(victim_id, pages_[frame_id].GetData());
    }
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();
    page_table_.erase(victim_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    goto found;
  }

  return nullptr;
found:
  Page *page = pages_ + frame_id;
  page_table_[page_id] = frame_id;
  page->page_id_ = page_id;
  ++page->pin_count_;
  replacer_->Pin(frame_id);
  return pages_ + frame_id;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  ValidatePageId(page_id);
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ != 0) {
    return false;
  }
  DeallocatePage(page_id);
  page_table_.erase(iter);
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->ResetMemory();
  replacer_->Unpin(frame_id);
  free_list_.emplace_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    Page *page = pages_ + frame_id;
    page->is_dirty_ |= is_dirty;
    if (--page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    return true;
  }
  return false;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub

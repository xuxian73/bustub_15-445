//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : frame2iter_{num_pages} {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (list_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = list_.back();
  list_.pop_back();
  frame2iter_[*frame_id] = std::list<frame_id_t>::iterator{};
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (!IsInList(frame_id)) {
    return;
  }
  list_.erase(frame2iter_[frame_id]);
  frame2iter_[frame_id] = std::list<frame_id_t>::iterator{};
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (!IsInList(frame_id)) {
    list_.push_front(frame_id);
    frame2iter_[frame_id] = list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return list_.size();
}

bool LRUReplacer::IsInList(frame_id_t frame_id) { return frame2iter_[frame_id] != std::list<frame_id_t>::iterator{}; }

}  // namespace bustub

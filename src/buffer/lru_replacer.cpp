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

LRUReplacer::LRUReplacer(size_t num_pages)
    : size_(0),
      value_(std::vector<int>(num_pages, 0)),
      exist_(std::vector<bool>(num_pages, false)),
      pinned_(std::vector<bool>(num_pages, false)),
      cur_(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  frame_id_t min = INT32_MAX;
  std::lock_guard<std::mutex> guard(mu_);
  int index = -1;
  if (size_ == 0) {
    frame_id = nullptr;
    return false;
  }
  for (size_t i = 0; i < value_.size(); ++i) {
    if (exist_[i] && !pinned_[i] && value_[i] < min) {
      index = i;
      min = value_[i];
    }
  }
  if (index == -1) {
    return false;
  }
  exist_[index] = false;
  value_[index] = 0;
  *frame_id = index;
  --size_;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  if (exist_[frame_id] && !pinned_[frame_id]) {
    pinned_[frame_id] = true;
    --size_;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  if (!exist_[frame_id]) {
    exist_[frame_id] = true;
    value_[frame_id] = ++cur_;
    pinned_[frame_id] = false;
    ++size_;
  } else if (pinned_[frame_id]) {
    pinned_[frame_id] = false;
    value_[frame_id] = ++cur_;
    ++size_;
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(mu_);
  return size_;
}

}  // namespace bustub

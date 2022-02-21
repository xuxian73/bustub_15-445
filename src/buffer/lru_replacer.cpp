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

LRUReplacer::LRUReplacer(size_t num_pages): 
size(0), value(std::vector<int>(num_pages, 0)), 
exist(std::vector<bool>(num_pages, false)), pinned(std::vector<bool>(num_pages, false)), cur(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    frame_id_t min = INT32_MAX;
    std::lock_guard<std::mutex> guard(mu);
    int index = -1;
    if (size == 0) {
        frame_id = nullptr;
        return false;
    }
    for (size_t i = 0; i < value.size(); ++i) {
        if (exist[i] && !pinned[i] && value[i] < min) {
            index = i;
            min = value[i];
        } 
    }
    if (index == -1) {
        return false;
    }
    exist[index] = false;
    value[index] = 0;
    *frame_id = index;
    --size;
    return true; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mu);
    if (!exist[frame_id] || pinned[frame_id]) {
        return;
    } else {
        pinned[frame_id] = true;
        --size;
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mu);
    if (!exist[frame_id]) {
        exist[frame_id] = true;
        value[frame_id] = ++cur;
        pinned[frame_id] = false;
        ++size;
    } else if (pinned[frame_id]) {
        pinned[frame_id] = false;
        value[frame_id] = ++cur;
        ++size;
    }
}

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> guard(mu);
    return size;
}

}  // namespace bustub

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

LRUReplacer::LRUReplacer(size_t num_pages) : size_(num_pages),frames_(num_pages){
  //初始的时候是没有unpined页的
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //仅返回链表头元素，并删除头元素
  if (list_unpinned_frames_.empty()) 
    return false;
  *frame_id = list_unpinned_frames_.front();
  list_unpinned_frames_.pop_front();
  frames_[*frame_id] = std::list<frame_id_t>::iterator{};
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(frame_id<0||frame_id>=static_cast<int>(size_)){
    LOG_WARN("Pin page %d of pool size %d",frame_id,static_cast<int>(size_));
    return; 
  }
  if (frames_[frame_id]==std::list<frame_id_t>::iterator{}) //已经pin住的支持多次pin
    return;
  list_unpinned_frames_.erase(frames_[frame_id]);
  frames_[frame_id] = std::list<frame_id_t>::iterator{}; //不能置nullptr置为未使用的迭代器
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(frame_id<0||frame_id>=static_cast<int>(size_)){
    LOG_WARN("Unpin page %d of pool size %d",frame_id,static_cast<int>(size_));
    return; 
  }
  //将新用过的加入链表尾
  if(frames_[frame_id]!=std::list<frame_id_t>::iterator{}){
    LOG_WARN("Unpin unpinned frame %d",frame_id);
    return;
  }
  list_unpinned_frames_.push_back(frame_id);
  frames_[frame_id] = prev(list_unpinned_frames_.end());
}

size_t LRUReplacer::Size() { 
  return list_unpinned_frames_.size();
}

}  // namespace bustub

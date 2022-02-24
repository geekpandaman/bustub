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

LRUReplacer::LRUReplacer(size_t num_pages) : size_(num_pages){
  for(int i =0;i!=num_pages;i++){
    list_unpinned_frames_.push_front(i);
    frames_[i] = list_unpinned_frames_.begin();
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //仅返回链表头元素，删除交给Pin来做
  if (list_unpinned_frames_.empty()) 
    return false;
  frame_id = &list_unpinned_frames_.front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frames_[frame_id]==std::list<frame_id_t>::iterator{})
    return;
  list_unpinned_frames_.erase(frames_[frame_id]);
  frames_[frame_id] = std::list<frame_id_t>::iterator{}; //不能置nullptr置为未使用的迭代器
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //将新用过的加入链表尾
  list_unpinned_frames_.push_back(frame_id);
  if(frames_[frame_id]!=std::list<frame_id_t>::iterator{}){
    LOG_ERROR("Unpin unpinned frame %d",frame_id);
  }
  frames_[frame_id] = prev(list_unpinned_frames_.end());
}

size_t LRUReplacer::Size() { 
  return list_unpinned_frames_.size();
}

}  // namespace bustub

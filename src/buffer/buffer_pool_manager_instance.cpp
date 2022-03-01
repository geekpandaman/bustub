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
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(!page_table_.count(page_id))
    return false;
  Page* pg = &pages_[page_table_[page_id]];
  if(pg->is_dirty_){
    this->disk_manager_->WritePage(pg->page_id_,pg->data_);
    pg->is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for(size_t i = 0;i!=pool_size_;i++){
    if(pages_[i].is_dirty_){
      this->disk_manager_->WritePage(pages_[i].page_id_,pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  //guard在构造时自动加锁，在析构时解锁，就避免了因为异常处理导致未解锁
  std::lock_guard<std::mutex> guard(latch_);
  page_id_t new_pg_id = AllocatePage();
  Page* frame = nullptr;
  frame_id_t frame_id;
  *page_id = new_pg_id;
  if(!free_list_.empty()){
    //从没有用过的列表中取frame
    frame_id = free_list_.front();
    free_list_.pop_front();
    frame = &pages_[frame_id];
  } else if(replacer_->Size()!=0){
    //从LRU中淘汰取frame
    replacer_->Victim(&frame_id);
    frame = &pages_[frame_id];
    FlushPgImp(frame->GetPageId()); //Victim后刷盘,lazy刷盘策略
    //reuse
    page_table_.erase(frame->page_id_);
    assert(frame->pin_count_==0);
    frame->ResetMemory();
  }else{
    //所有页面均被pin住
    return nullptr;
  }
  //设置page_tables_,pin住页面返回
  frame->page_id_ = new_pg_id;
  page_table_[new_pg_id] = frame_id;
  replacer_->Pin(frame_id);
  return frame;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  //相比NewPgImp首先要判断是否在内存中
  //pin_count++?
  //拿出frame的逻辑是一样的
  //相比NewPgImp拿出之后要从磁盘读
  std::lock_guard<std::mutex> guard(latch_);
  Page* frame = nullptr;
  if(page_table_.count(page_id)){
    //TODO 是否要pincount++
    frame_id_t frame_id=page_table_[page_id];
    replacer_->Pin(frame_id);
    frame = &pages_[frame_id];
  }else{
    //这段代码和NewPgImp中的逻辑是一样的
    frame_id_t frame_id;
    if(!free_list_.empty()){
      //从没有用过的列表中取frame
      frame_id = free_list_.front();
      free_list_.pop_front();
      frame = &pages_[frame_id];
    } else if(replacer_->Size()!=0){
      //从LRU中淘汰取frame
      replacer_->Victim(&frame_id);
      frame = &pages_[frame_id];
      FlushPgImp(frame->GetPageId()); //Victim后刷盘,lazy刷盘策略
      //reuse
      page_table_.erase(frame->page_id_);
      assert(frame->pin_count_==0);
      frame->ResetMemory();
    }else{
      //所有页面均被pin住
      return nullptr;
    }
    //设置page_tables_,pin住页面返回
    frame->page_id_ = page_id;
    page_table_[page_id] = frame_id;
    replacer_->Pin(frame_id);
    //从磁盘读
    disk_manager_->ReadPage(page_id,frame->data_);
  }
  return frame;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  if(!page_table_.count(page_id))
    return true;
  DeallocatePage(page_id);
  frame_id_t frame_id = page_table_[page_id];
  Page* frame = &pages_[frame_id];
  if(frame->pin_count_!=0)
    return false;
  //删除页面
  //TODO disk上的删除
  page_table_.erase(page_id);
  frame->ResetMemory();
  frame->is_dirty_ = false;
  frame->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  //会在pincount为0的时候被调用
  //unpin的时候不刷盘
  //dirty参数表示这个page在pin住的过程中有没有被修改
  std::lock_guard<std::mutex> guard(latch_);
  if(!page_table_.count(page_id))
    return false;
  frame_id_t frame_id = page_table_[page_id];
  // if(pages_[frame_id].pin_count_<=0)
  //   return false;
  pages_[frame_id].is_dirty_ = is_dirty;
  replacer_->Unpin(frame_id);
  return true;
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

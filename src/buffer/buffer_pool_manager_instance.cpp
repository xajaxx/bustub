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
  // Page都会初始化？
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
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}
/** Fetch the requested page from the buffer pool. 
 * 应该就是线程调用该页面，如果存在，那么直接pin一下，返回地址
 * 如果不存在，找一个就去空闲列表找一个使用，如果空闲列表没了
 * 那么就剔除一个页面，然后如果是脏页，就刷回去，然后返回对应的page
 */
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page *p = nullptr;
  replacer_->Pin(page_id);
  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}
/**
 *  Unpin the target page from the buffer pool. 
 *  return false if the page pin count is <= 0 before this call, true otherwise
 * 在上层的视角来看，就是一个线程用完这个page之后，就会触发unpin
 * 然后在BMP里面，就会把这个pagepincount--，然后如果 == 0，就会在LRU中unpin，如果大于零则不会处理
 * 至于is_dirty，就是上层的thread传入的，需要更新该page的is_dirty状态
 * 还需要查看pincount，如果page的pincount是0，那么就把他的id放入lru里面
 * （也就是说，page_里面存的是具体的page数据和id，然后page_的下标是frame_id）
 */
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  pg_latch_.lock();
  pt_latch_.lock();

  // 不在BMP，直接返回成功
  if (!page_table_.count(page_id)){
    pt_latch_.unlock();
    pg_latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if(pages_[frame_id].GetPinCount()){
    pages_[frame_id].DecreasePinCount();
  }

  // pincount 为零则LRU执行Unpin
  if(!pages_[frame_id].GetPinCount()){
    replacer_->Unpin(frame_id);
  }

  // 设置脏页或者否
  if(is_dirty){
    pages_[frame_id].SetDirty();
  } else {
    pages_[frame_id].SetUnDirty();
  }

  pt_latch_.unlock();
  pg_latch_.unlock();
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

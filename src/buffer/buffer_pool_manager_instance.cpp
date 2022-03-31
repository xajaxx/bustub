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
  if(!page_table_.count(page_id)){
    return false;
  }

  frame_id_t f_id = page_table_[page_id];
  if(!pages_[f_id].IsDirty()){
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[f_id].data_);
  pages_[f_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> pg_lg(pg_latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if(pages_[i].IsDirty()){
      FlushPgImp(pages_[i].page_id_);
    }
  }
}

// 从lru获取一个f_id，并且根据其是否为脏页刷回到磁盘中
frame_id_t BufferPoolManagerInstance::GetPageFromLRU(){
  frame_id_t f_id; 
  if (!replacer_->Victim(&f_id)) {
    // 此时可能是所有的page都在被线程读写，并发量达到最高，LRU没有页面等待被淘汰
    return INVALID_PAGE_ID;
  }

  page_id_t p_id = pages_[f_id].GetPageId();

  if(pages_[f_id].IsDirty()){
    if (!FlushPgImp(p_id)){
      // 刷入磁盘脏页失败
      return INVALID_PAGE_ID;
    }
  }
  return f_id;
}

/*
********************************************************************************
newpageip和FetchPgImp有什么区别？
：newpageip是用来新建一个页面，干净的，返回给上层
：FetchPgImp则是，上层之前使用过这个页面，
  之前可能有一些原因比如不再使用了，放入lru或者被刷新到磁盘了，这个时候需要再拿出来

********************************************************************************
*/

// 为上层提供一个借口，freelist如果有空闲的，那么直接使用freelist的页框，如果freelist没有
// 那么lru找，lru是用来暂存的，是一些线程使用过的页面
// 如果这两个都没有，那么说明所有page都在被线程使用（因为所有不被线程使用的page才会被放入lru）
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock<std::mutex> free_ul(free_latch_);
  frame_id_t f_id;

  if(free_list_.empty() && replacer_->Size() == 0){
    return nullptr;
  }

  if(!free_list_.empty()){
    f_id = free_list_.front();
    free_list_.pop_front();
    free_ul.unlock();
  } else {
    if((f_id = GetPageFromLRU()) == INVALID_PAGE_ID){
      return nullptr;
    }
  }
  std::lock_guard<std::mutex> pg_lg(pg_latch_);
  std::lock_guard<std::mutex> pt_lg(pt_latch_);
  page_id_t p_id = AllocatePage();
  pages_[f_id].ResetMemory();
  pages_[f_id].is_dirty_ = false;
  pages_[f_id].pin_count_ = 1;
  pages_[f_id].page_id_ = p_id;
  page_table_.insert({p_id, f_id});

  *page_id = p_id;

  return &pages_[f_id];
}
/** Fetch the requested page from the buffer pool. 
 * 应该就是线程调用该页面，如果存在，那么直接pin一下，返回地址
 * 如果不存在，找一个就去空闲列表找一个使用，如果空闲列表没了
 * 那么就剔除一个页面，然后如果是脏页，就刷回去，然后返回对应的page
 * ！！！ 如果更新了新的页，不要忘了更新page_table
 */
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> pg_lg(pg_latch_);
  std::lock_guard<std::mutex> pt_lg(pt_latch_);

  if(page_table_.count(page_id)){
    frame_id_t f_id = page_table_[page_id];
    replacer_->Pin(f_id);
    pages_[f_id].pin_count_++;
    return &pages_[page_table_[page_id]];
  }

  frame_id_t f_id;
  std::unique_lock<std::mutex> free_ul(free_latch_);
  if ( !free_list_.empty() ){
    f_id = free_list_.front();
    free_list_.pop_front();
    pages_[f_id].pin_count_++;

    return &pages_[f_id];
  } else {
    if((f_id = GetPageFromLRU()) == INVALID_PAGE_ID){
      return nullptr;
    }
  }
  free_ul.unlock();
  page_table_.erase(page_id);
  // 插入新的 page_id 与 frame_id 对
  page_table_.insert({page_id, f_id});
  disk_manager_->ReadPage(page_id, pages_[f_id].data_);
  pages_[f_id].pin_count_ = 1;
  pages_[f_id].is_dirty_ = false;
  pages_[f_id].page_id_ = page_id;

  return &pages_[f_id];
}

//  不需要刷盘，因为delete是一个上层调用的动作
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> pg_lg(pg_latch_);
  std::lock_guard<std::mutex> pt_lg(pt_latch_);

  if(!page_table_.count(page_id)){
    return true;
  }

  frame_id_t f_id = page_table_[page_id];

  if(pages_[f_id].GetPinCount()){
    return false;
  }

  // disk_manager_->DeallocatePage(page_id);
  pages_[f_id].ResetMemory();
  pages_[f_id].is_dirty_ = false;
  pages_[f_id].page_id_ = INVALID_PAGE_ID;
  page_table_.erase(page_id);

  std::lock_guard<std::mutex> free_lg(free_latch_);
  free_list_.push_back(f_id);
  return true;
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
  std::lock_guard<std::mutex> pg_lg(pg_latch_);
  std::unique_lock<std::mutex> pt_lg(pt_latch_);

  // 不在BMP，直接返回成功
  if (!page_table_.count(page_id)){
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  pt_lg.unlock();

  if (pages_[frame_id].GetPinCount()) {
    pages_[frame_id].pin_count_--;
  }

  // pincount 为零则LRU执行Unpin
  if(!pages_[frame_id].GetPinCount()){
    replacer_->Unpin(frame_id);
  }

  // 设置脏页或者否
  pages_[frame_id].is_dirty_ = is_dirty;

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

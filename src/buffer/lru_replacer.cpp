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

#include <mutex>
#include "buffer/lru_replacer.h"

namespace bustub {

/*
 * 感觉这里的LRU是一个Buffer Pool Manager 的辅助工具
 * 如果一个page pin了，那么就是调入BPM中被读写，然后从LRU淘汰
 * 如果一个Page unpin了，那么就是从BPM中放回来，然后放入LRU末尾，如果unpin的尺寸超出，那么还是淘汰开头的
 * 注意，unpin两次一个页面，第二次无需做任何操作，unpin是幂等的
 * victim是直接扔掉最开始的，因为list开始是最早没有使用的，在队尾的都是刚刚读写完，放入list中的
 */
LRUReplacer::LRUReplacer(size_t num_pages) {
    // 多线程并发，防止创建多个head节点
    if (head){
      return;
    }
    std::lock_guard<std::mutex> lg(m);
    head = new ListNode(-1);
    tail = new ListNode(-1);
    size_page = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lg(m);
  if (size_page == 0) {
    return false;
  }
  auto page_ = head->next;
  *frame_id = page_->frame_id;
  delete (page_);
  --size_page;
  return true;
}

/* 该页面有线程正在读写，应该把页面从LRU移除，如果没有该页面，那么直接忽略 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(m);
    if (size_page == 0 || !cachePage.count(frame_id)) {
        return;
    }
    auto page_ = cachePage[frame_id];
    delete (page_);
    --size_page;
}
/* 没有线程读写了，放入LRU等待淘汰，直接把当前id放入队尾 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(m);
    if(cachePage.count(frame_id)){
      return;
    }
    auto node = new ListNode(frame_id);
    tail->prev->next = node;
    node->prev = tail->prev;
    tail->prev = node;
    node->next = tail;
    ++size_page;
}

size_t LRUReplacer::Size() {
    return size_page;
}

void LRUReplacer::deleteNode(ListNode *node) {
    node->next->prev = node->prev;
    node->prev->next = node->next;
    delete node;
}

}  // namespace bustub

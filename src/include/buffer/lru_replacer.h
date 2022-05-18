//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * 存放节点.
 */
struct ListNode {
    frame_id_t frame_id;
    ListNode *next;
    ListNode *prev;
    ListNode() : frame_id(0), next(nullptr), prev(nullptr){}
    ListNode(int id) : frame_id(id), next(nullptr), prev(nullptr){}
    // ListNode(int val_) : val(val_), next(nullptr), prev(nullptr){}
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  void moveToEnd(frame_id_t frame_id);

  size_t Size() override;

 protected:
  void AddNode(ListNode *node);

  void deleteNode(ListNode *node);
  
 private:
  // TODO(student): implement me!
  std::mutex m;
  std::unordered_map<frame_id_t, ListNode *> cachePage;
  ListNode *head = nullptr;
  ListNode *tail = nullptr;
  size_t max_size_;
  size_t size_;
};

}  // namespace bustub

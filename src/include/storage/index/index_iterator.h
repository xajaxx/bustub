//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page *node, int idx, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return p_id == itr.p_id && idx == itr.idx; }

  bool operator!=(const IndexIterator &itr) const { 
    // throw std::runtime_error("unimplemented");
    return p_id != itr.p_id || idx != itr.idx;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node;
  int idx = 0;
  page_id_t p_id;
};

}  // namespace bustub


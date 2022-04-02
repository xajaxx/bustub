/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : buffer_pool_manager_ (nullptr),
                                        leaf_node(nullptr), 
                                        idx(-1) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *node, int idx, BufferPoolManager *buffer_pool_manager) 
                                    : buffer_pool_manager_ (buffer_pool_manager), idx(idx) {
  leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node->GetData());
  if(leaf_node != nullptr){
    p_id = leaf_node->GetPageId();
  } else {
    p_id = INVALID_PAGE_ID;
  }
  
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (p_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(p_id, false);
  }
  // delete buffer_pool_manager_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() {
  if(p_id == INVALID_PAGE_ID){
    return true;
  }
  // if (leaf_node->GetNextPageId() == INVALID_PAGE_ID && idx == leaf_node->GetSize()) {
  //   return true;
  // }
  return false;
    // throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    // throw std::runtime_error("unimplemented");
    return leaf_node->GetItem(idx);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    // throw std::runtime_error("unimplemented");
  ++idx;
  if (idx == leaf_node->GetSize()) {
    idx = 0;
    page_id_t nxt_p_id = leaf_node->GetNextPageId();
    if(nxt_p_id == INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(p_id, false);
      leaf_node = nullptr;
    } else {
      Page *page = buffer_pool_manager_->FetchPage(nxt_p_id);
      leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    }
    buffer_pool_manager_->UnpinPage(p_id, false);
    p_id = nxt_p_id;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub


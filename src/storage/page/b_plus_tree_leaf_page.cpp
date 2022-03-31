//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
//   prev_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
IndexPageType GetPageType() { return IndexPageType::LEAF_PAGE; }
/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

// INDEX_TEMPLATE_ARGUMENTS
// page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrevPageId() const { return prev_page_id_; }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrevPageId(page_id_t prev_page_id) { prev_page_id_ = prev_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
//   if (GetSize() == 0){
//     return 0;
//   }
  int low = -1, high = GetSize();
  while(low + 1 < high){
    int mid = (low + high) >> 1;
    int cmp = comparator(key, KeyAt(mid));
    if (cmp == 0) return mid;
    if(cmp == -1){
      // key < mid
      high = mid;
    } else {
      low = mid;
    }
  }
  // return (low + high) >= 0 ? ((low + high) >> 1) : 0;
  return low + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // KeyType key{};
  // if(index < 1 && index >= GetSize()){
  //   return key;
  // }
  
  // key = array_[index].first;
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  // if (index < GetSize() && index >= 0) {
  //   return {};
  // }
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int pos, const KeyType &key, const ValueType &value){
  int size = GetSize();
  for(int i = size; i > pos; --i){
    array_[i] = array_[i - 1];
  }
  array_[pos] = MappingType{key, value};
  IncreaseSize(1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 检查数量
  if(GetSize() == GetMaxSize()){
    std::cout << "Now the size of leaf is: " << GetSize() << ", max size is: " << GetMaxSize() << std::endl;
    return GetSize();
  }

  int idx = KeyIndex(key, comparator);

  InsertAt(idx, key, value);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * 把当前页面的数据拷贝一半到recipient中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int move_size = GetSize() / 2;
  for(int i = GetSize() - move_size; i < GetSize(); ++i){
    recipient->CopyLastFrom(array_[i]);
  }
//   recipient->IncreaseSize(move_size);
  IncreaseSize(-1 * move_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 * 应该是从item拷贝到当前的array中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  if(GetSize() + size > GetMaxSize()){
    std::cout << "Size overflow, after insert, the size is: " << (GetSize() + size) << ", maxsize is: " << GetMaxSize()
              << std::endl;
    return;
  }

  for (int pos = GetSize(), i = 0; i < size; ++i, ++pos){
    array_[pos] = *(items + i);
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 * 找到对应的key，并且把对应的value值放入*value中
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int idx = KeyIndex(key, comparator);
  if(comparator(key, array_[idx].first) != 0){
    return false;
  }
  *value = array_[idx].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; ++i){
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int idx = KeyIndex(key, comparator);
  if(comparator(key, array_[idx].first) != 0){
    return GetSize();
  }
  for (int i = idx; i < GetSize() - 1; ++i){
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  if(recipient->GetSize() + GetSize() > recipient->GetMaxSize()){
    std::cout << "should not merge, beacuse the size after merge is: " << recipient->GetSize() + GetSize() << std::endl;
    return;
  }
//   // 当前节点是接收节点的右节点，应该插入到recipient的末尾
//   if(recipient->GetNextPageId() == GetPageId()){
//     int pos = recipient->GetSize();
//     for (int i = 0; i < GetSize(); ++i) {
//       recipient->array_[pos ++] = array_[i];
//     }
//   } else {
//     // 当前节点是待合并节点的左节点，应该插入到开头
//     int cur_size = GetSize();
//     int merge_size = cur_size + recipient->GetSize();
//     for (int i = merge_size - 1; i >= GetSize(); --i) {
//       recipient->array_[i] = recipient->array_[i - cur_size];
//     }

//     for(int i = 0; i < cur_size; ++i){
//       recipient->array_[i] = array_[i];
//     }
//   }
  // 经过思考，应该是：
  // 无论如何，都是左边合并到右边，所以，recipient一直都是他的左边
  // 如，删除的是父亲节点的最左边的孩子节点，那么上层应该控制，选择把这个节点右边的兄弟节点合并到这个节点中
  // 而且，一旦发生合并，也就是说，该节点的左右节点都不可借了，所以合并肯定是没问题的
  int size = GetSize();
  for(int i = 0; i < size; ++i){
    recipient->CopyLastFrom(array_[i]);
  }
  recipient->IncreaseSize(size);
  // 链表不可以断掉
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);

  /*
  ****************************************************************************************
    // 更新双向链表的操作应该交给上层的节点，因为：
    // 如果需要合并的节点在当前节点的左边，那么是无法设置右节点的prev的，同样对于待合并节点是右节点。

    // // 如果是合并的左兄弟，那么把prev -> next = next
    // if (recipient->GetPageId() == GetPrevPageId()){
    //   recipient->SetNextPageId(GetNextPageId());
    // } else {
    //   // 否则，是当前节点的右兄弟，把pre->next

    // }
    // recipient->SetNextPageId(GetNextPageId());
    // SetNextPageId(recipient->GetPageId());
  ****************************************************************************************
  */
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 * 把当前节点的第一个节点借给左节点（recipient）的最后一个
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  if(recipient->GetSize() == recipient->GetMaxSize()){
    std::cout << "page_id = " << recipient->GetPageId() << ", size overflow" << std::endl;
    return;
  }
  recipient->CopyLastFrom(array_[0]);
  for (int i = 0; i < GetSize() - 1; ++i){
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int pos = GetSize();
  InsertAt(pos, item.first, item.second);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 * 把当前节点的最后一个借给右节点（recipient）的第一个
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  if(recipient->GetSize() + 1 > recipient->GetMaxSize()){
    std::cout << "page_id = " << recipient->GetPageId() << ", size overflow" << std::endl;
    return;
  }
  for(int i = recipient->GetSize(); i > 0; --i){
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    InsertAt(0, item.first, item.second);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size){
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_id);
  SetSize(0);
}


INDEX_TEMPLATE_ARGUMENTS
IndexPageType GetPageType() { return IndexPageType::INTERNAL_PAGE; }
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{};
  if(index < 1 && index >= GetMaxSize()){
    return key;
  }
  
  key = array_[index].first;
  return key;
}

// 因为是set，所以直接替换原来的key就可以，如果是插入，则是insert
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if(index < 1 && index >= GetMaxSize()){
    return;
  }
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if(index < 0 && index >= GetSize()){
    return;
  }
  array_[index].second = value;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 * const代表函数是一个只读函数
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; ++i) {
    if(array_[i].second == value){
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
//   if(index < 0 && index > GetSize()){
//     return 0;
//   }

  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
数据的逻辑存储格式如下
 (k[0]为空)      5(k[1])             10(k[2]) 
        / v[0]             | v[1]               \ v[2]
       1,2,3,4            5,6,9               10,11,15

所以IndexLookup应该返回第一个大于等于key值的索引

*/

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexLookup(const KeyType &key, const KeyComparator &comparator) const{
  // 0, size_都不存数据
  int low = 0, high = GetSize();
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
  return ((low + high) >> 1);
}

/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int idx = IndexLookup(key, comparator);
  return ValueAt(idx);
  // return nullptr;
//   ValueType v{};
//   return v;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * old 是 0， new 是1？
 * 这个方法当且仅当使用在b_plus_tree.cpp中
 * 生成一个新的根节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetSize(2);
}

// 返回插入后的大小
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int idx, const KeyType &new_key, const ValueType &new_value){
  int size = GetSize();
  for(int i = size; i > idx; --i){
    array_[i] = array_[i - 1];
  }
  array_[idx] = MappingType{new_key, new_value};
  IncreaseSize(1);
  return GetSize();
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int idx = ValueIndex(old_value);
  array_[idx] = MappingType{new_key, new_value};
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * 把当前页面的一半的kv对，移到recipient中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int move_size = GetSize() / 2;
  for(int i = move_size; i < GetSize(); ++i){
    recipient->CopyLastFrom(array_[i], buffer_pool_manager);
  }
  IncreaseSize(-1 * move_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for(int i = 0; i < size; ++i){
    CopyLastFrom(items[i], buffer_pool_manager);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; ++i){
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  if(GetSize() != 1){
    return INVALID_PAGE_ID;
  }

  page_id_t p_id = ValueAt(0);
  Remove(0);

  return p_id;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 永远是从当前节点合并到左节点上
  // 对于middle_key，对应的value则是当前节点的value[0]
  MappingType pair = MappingType{middle_key, array_[0].second};
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  int size = GetSize();
  for (int i = 0; i < size; ++i){
    recipient->CopyLastFrom(array_[i], buffer_pool_manager);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 把当前的节点借给左兄弟的时候，需要把父节点的对应节点放入到recipient中，并且把first k值给父节点
  // 所以对于当前节点，需要做的就是把v[0]与middle_key组成pair放到recipient末尾，由上层移除当前首个节点
  MappingType pair = MappingType{middle_key, array_[0].second};
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  // middle_key = array_[1].first;
  recipient->SetChildParent(array_[0].second, buffer_pool_manager);
  Remove(0);
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array_[size] = pair;
  SetChildParent(pair.second, buffer_pool_manager);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // k给父亲节点，v给右兄弟
  int size = GetSize();
  MappingType pair = MappingType{middle_key, array_[size - 1].second};
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
  // middle_key = array_[size - 1].first;
  recipient->SetChildParent(array_[size - 1].second, buffer_pool_manager);
  IncreaseSize(-1);
}

// 把对应page的父节点设置成当前节点，并且设置脏页标记，刷回磁盘
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetChildParent(ValueType p_id, BufferPoolManager *buffer_pool_manager){
  Page *page_ = buffer_pool_manager->FetchPage(p_id);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page_);
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(p_id, true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // k v应该分开处理
  int size = GetSize();
  for (int i = size; i > 1; --i){
    array_[i] = array_[i - 1];
  }
  array_[1].first = pair.first;
  array_[1].second = array_[0].second;
  array_[0].second = pair.second;
  SetChildParent(pair.second, buffer_pool_manager);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <thread>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  root_latch_.RLock();
  Page *raw_leaf_page = FindLeafPage(key, transaction, 0, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(raw_leaf_page);
  // int idx = leaf_page->KeyIndex(key, comparator_);
  // 查找成功
  bool ans = false;
  ValueType val;
  if (leaf_page->Lookup(key, &val, comparator_)) {
    result->emplace_back(val);
    ans = true;
  }
  if(transaction != nullptr){
    ReleaseTxnPage(transaction, 0);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  root_latch_.RUnlock();
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  bool ans = false;
  root_latch_.WLock();
  if (IsEmpty()) {
    // auto lock = transaction->GetExclusiveLockSet();
    // std::unique_lock<std::mutex> ul();
    StartNewTree(key, value);
    ans = true;
  } else {
    ans = InsertIntoLeaf(key, value, transaction);
  }
  ReleaseTxnPage(transaction, 1);
  root_latch_.WUnlock();
  return ans;
  // return false;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(new_page == nullptr){
    throw("out of memory!");
  }
  UpdateRootPageId(root_page_id_);

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *raw_leaf_page = FindLeafPage(key, transaction, 1, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(raw_leaf_page);
  int idx = leaf_page->KeyIndex(key, comparator_);
  // 有重复键，没有值的时候idx小于0
  if(idx >= 0 && comparator_(leaf_page->KeyAt(idx), key) == 0){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  leaf_page->InsertAt(idx, key, value);
  // 处理split
  if(leaf_page->GetSize() >= leaf_page->GetMaxSize()){
    Split<BPlusTreePage>(leaf_page, transaction);
  }
  if(transaction != nullptr){
    ReleaseTxnPage(transaction, 1);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);
  N *ans_node;

  if(node->IsLeafPage()){
    // 叶子节点的分裂
    LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    LeafPage *old_node = reinterpret_cast<LeafPage *>(node);
    new_node->Init(new_page_id, old_node->GetParentPageId(), leaf_max_size_);
    old_node->MoveHalfTo(new_node);
    // 双向链表
    new_node->SetNextPageId(old_node->GetNextPageId());
    old_node->SetNextPageId(new_node->GetPageId());
    auto key = new_node->KeyAt(0);
    InsertIntoParent(old_node, key, new_node, transaction);
    ans_node = reinterpret_cast<N *>(new_node);
  } else {
    // internal page 的分裂
    InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    InternalPage *old_node = reinterpret_cast<InternalPage *>(node);
    new_node->Init(new_page_id, old_node->GetParentPageId(), internal_max_size_);
    old_node->MoveHalfTo(new_node, buffer_pool_manager_);
    auto key = new_node->KeyAt(0);
    InsertIntoParent(old_node, key, new_node, transaction);
    ans_node = reinterpret_cast<N *>(new_node);
  }
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  return ans_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // parent节点一定是internal page type
  // 先找到old_node所在page的value的index，然后再在这个index后面插入key，new_node->pageid的kv对
  // 插入完成后，检查是不是还需要检查需要递归的插入

  // 还是得分成两种情况，本身是internal page 和本身是leaf page的
  // 假如本身就是internal page，那么插入到父节点的时候，还需要把心节点的中间节点给父节点
  // 还有一种情况是本身是根节点，难么需要调用PopulateNewRoot方法
  if(old_node->IsRootPage()){
    // 满了之后，因为没有上层节点，所以得重新申请一个新的节点当做新的root节点
    // 然后再调用PopulateNewRoot
    page_id_t new_root_id;
    Page *raw_root = buffer_pool_manager_->NewPage(&new_root_id);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(raw_root->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_id;
    UpdateRootPageId(new_root_id);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    new_root->SetParentPageId(INVALID_PAGE_ID);

    // 更新当前节点与新节点的parentid
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  } else {
    // 普通的internal节点
    // 找出对应的page，然后转换成internal page类型
    page_id_t parent_p_id = old_node->GetParentPageId();
    Page *raw_parent_page = buffer_pool_manager_->FetchPage(parent_p_id);
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(raw_parent_page->GetData());

    // 将new_node的第一个节点插入到parent节点中，在old_node后面，应该为idx + 1
    int idx = parent_page->ValueIndex(old_node->GetPageId());
    int new_size = parent_page->InsertAt(idx + 1, key, new_node->GetPageId());
    // if(old_node->GetPageType() == IndexPageType::INTERNAL_PAGE){
    //   // 如果是internal节点，还需要删除new_node的第一个节点，因为与父节点重合
    //   new_node->Remove(0);
    // }
    // 检查父节点是否需要继续分裂
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    if (new_size >= parent_page->GetMaxSize() + 1) {
      Split<BPlusTreePage>(parent_page);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  
  if (IsEmpty()) {
    root_latch_.WUnlock();
    return;
  }

  Page *raw_target_page = FindLeafPage(key, transaction, 2, false);
  LeafPage *target_page = reinterpret_cast<LeafPage *>(raw_target_page);
  page_id_t target_id = target_page->GetPageId();
  int idx = target_page->KeyIndex(key, comparator_);
  if(idx >= target_page->GetSize() || comparator_(key, target_page->KeyAt(idx)) != 0){
    if(transaction != nullptr){
      // std::cout << "thread_id = " << std::this_thread::get_id();
      // std::cout << ", delete key = " << key << " failed. " << std::endl;
      ReleaseTxnPage(transaction, 2);
    }
    root_latch_.WUnlock();
    return;
  }

  target_page->Remove(idx);
  if(target_page->GetSize() < target_page->GetMinSize()){
    CoalesceOrRedistribute<LeafPage>(target_page, transaction);
  }
  if(transaction != nullptr){
    ReleaseTxnPage(transaction, 2);
    DeletePages(transaction);
    // std::cout << "thread_id = " << std::this_thread::get_id();
    // std::cout << ", delete key = " << key << " succeed. " << std::endl;
  }
  buffer_pool_manager_->UnpinPage(target_id, true);
  root_latch_.WUnlock();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 找左右节点“借”kv值，此函数即：找左、右
 * 如果一个页面被删除，那么返回成功，没有删除的，就返回失败 —— 此处方便事务处理
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // if(node->GetPageType() == IndexPageType::LEAF_PAGE){
    // 查看左右节点是否有对应的值
  // int cur_size = node->GetSize();
  if(node->IsRootPage()){
    return AdjustRoot(node);
  }

  // 找到父节点
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int idx = parent->ValueIndex(node->GetPageId());

  N *l_page = nullptr;
  N *r_page = nullptr;
  page_id_t l_p_id = INVALID_PAGE_ID, r_p_id = INVALID_PAGE_ID;
  page_id_t parent_id = parent->GetPageId();

  if(idx > 0){
    // 有左节点
    l_p_id = parent->ValueAt(idx - 1);
    Page *raw_l_page = buffer_pool_manager_->FetchPage(l_p_id);
    l_page = reinterpret_cast<N *>(raw_l_page->GetData());
    if (l_page->GetSize() > l_page->GetMinSize()) {
      Redistribute<N>(l_page, node, 1);
      buffer_pool_manager_->UnpinPage(l_p_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, false);
      return false;
    }
  } else if(idx < parent->GetSize() - 1){
    // 找右节点
    r_p_id = parent->ValueAt(idx + 1);
    Page *raw_r_page = buffer_pool_manager_->FetchPage(r_p_id);
    r_page = reinterpret_cast<N *>(raw_r_page->GetData());
    if(r_page->GetSize() > r_page->GetMinSize()){
      Redistribute<N>(r_page, node, 0);
      buffer_pool_manager_->UnpinPage(r_p_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, false);
      return false;
    }
  }

  bool ans = false;
  // 到这里说明左右节点无法借到kv值，那么只有与左or右节点进行合并了
  if (l_page) {
    Coalesce<N>(&l_page, &node, &parent, 1, transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(l_p_id, true);
    if(r_page){
      buffer_pool_manager_->UnpinPage(r_p_id, false);
    }
    ans = true;
  } else if(r_page){
    Coalesce<N>(&r_page, &node, &parent, 0, transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(r_p_id, true);
    ans = true;
  }
  return ans;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * 
 * 把邻居节点的所有数据合并到node中，并且使用buffer pool刷回page
 * 同时还需要更新父节点
 * 如果节点也需要处理，那么就递归的处理
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 * 
 * 返回值：如果parent节点也是应该被删除，那么就返回true，否则就返回false
 * index - 1 表示，neighber是左边的节点
 * index - 0 表示，neighber是右边的节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  int node_idx = (*parent)->ValueIndex((*node)->GetPageId());
  bool ret = false;
  
  if ((*node)->IsLeafPage()) {
    LeafPage **imp_neighbor_page = reinterpret_cast<LeafPage **>(neighbor_node);
    LeafPage **imp_node_page = reinterpret_cast<LeafPage **>(node);
    if(index == 1){
      // neighber是node左边的节点
      (*imp_node_page)->MoveAllTo(*imp_neighbor_page);
      buffer_pool_manager_->DeletePage((*imp_node_page)->GetPageId());
      buffer_pool_manager_->UnpinPage((*imp_neighbor_page)->GetPageId(), true);
      (*parent)->Remove(node_idx);
    } else {
      // neighber是node右边的节点
      (*imp_neighbor_page)->MoveAllTo(*imp_node_page);
      buffer_pool_manager_->DeletePage((*imp_neighbor_page)->GetPageId());
      buffer_pool_manager_->UnpinPage((*imp_node_page)->GetPageId(), true);
      (*parent)->Remove(node_idx + 1);
    }
  } else {
    // 本身就是internal节点，那么就必须使用middle key
    InternalPage **imp_neighbor_page = reinterpret_cast<InternalPage **>(neighbor_node);
    InternalPage **imp_node_page = reinterpret_cast<InternalPage **>(node);
    if(index == 1){
      // neighber是node左边的节点
      auto middle_key = (*parent)->KeyAt(node_idx);

      (*imp_node_page)->MoveAllTo((*imp_neighbor_page), middle_key, buffer_pool_manager_);
      transaction->AddIntoDeletedPageSet((*imp_node_page)->GetPageId());
      // buffer_pool_manager_->DeletePage((*imp_node_page)->GetPageId());
      buffer_pool_manager_->UnpinPage((*imp_neighbor_page)->GetPageId(), true);
      (*parent)->Remove(node_idx);
    } else {
      // neighber是node右边的节点
      auto middle_key = (*parent)->KeyAt(node_idx + 1);

      (*imp_neighbor_page)->MoveAllTo(*imp_node_page, middle_key, buffer_pool_manager_);
      transaction->AddIntoDeletedPageSet((*imp_neighbor_page)->GetPageId());
      // buffer_pool_manager_->DeletePage((*imp_neighbor_page)->GetPageId());
      buffer_pool_manager_->UnpinPage((*imp_node_page)->GetPageId(), true);
      (*parent)->Remove(node_idx + 1);
    }
  }
  // 检查父节点是否需要调整
  if((*parent)->GetSize() < (*parent)->GetMinSize()){
    ret = CoalesceOrRedistribute((*parent), transaction);
  }
  return ret;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * 
 * 重新分配kv值
 * 是 coalesce 或者 Redistribute（递归？） 的输入
 * index - 1 表示，neighber是左边的节点
 * index - 0 表示，neighber是右边的节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // 分两种情况，叶子节点和非叶子节点
  // 叶子节点，如果是借的是右边的，那么应该更新父节点对应的key为右边新的节点；如果是借的是左边的，那么更新父节点为本节点key
  // 非叶子节点，需要把父节点的值拿下来，左右节点的放到对应的位置去
  /*
  **********只应该出现在CoalesceOrRedistribute中*************
  // if(node->IsRootPage()){
  //   AdjustRoot(node);
  //   return;
  // }
  **********只应该出现在CoalesceOrRedistribute中*******************
  */ 
  page_id_t parent_p_id = node->GetParentPageId();
  Page *raw_parent_page = buffer_pool_manager_->FetchPage(parent_p_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(raw_parent_page->GetData());

  // node 在 parent节点的index值，用于更新parent对应的key值
  int node_idx = parent_page->ValueIndex(node->GetPageId());

  if(node->IsLeafPage()){
    // 叶子节点
    LeafPage *imp_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *imp_neighbor_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if(index == 1){
      // neighbor 是 node 左边节点
      imp_neighbor_node->MoveLastToFrontOf(imp_node);
      parent_page->SetKeyAt(node_idx, imp_node->KeyAt(0));
    } else {
      // neighbor 是 node 右边节点
      imp_neighbor_node->MoveFirstToEndOf(imp_node);
      parent_page->SetKeyAt(node_idx + 1, imp_neighbor_node->KeyAt(0));
    }
  } else {
    // 非叶子节点
    InternalPage *imp_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *imp_neighbor_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if(index == 1){
      // neighbor 是 node 左边节点
      auto middle_key = parent_page->KeyAt(node_idx);
      auto new_mid_key = imp_neighbor_node->KeyAt(imp_neighbor_node->GetSize() - 1);
      imp_neighbor_node->MoveLastToFrontOf(imp_node, middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(node_idx, new_mid_key);
    } else {
      // neighbor 是 node 右边节点
      auto middle_key = parent_page->KeyAt(node_idx + 1);
      auto new_mid_key = imp_neighbor_node->KeyAt(1);
      imp_neighbor_node->MoveFirstToEndOf(imp_node, middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(node_idx, new_mid_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_p_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 第零种情况：大于等于一个节点，直接返回
  if (old_root_node->GetSize() > 0){
    return true;
  } else {
    if(!old_root_node->IsLeafPage()){
      // 第一种情况：当前root还有孩子节点
      InternalPage *imp_old_root_node = reinterpret_cast<InternalPage *>(old_root_node);
      page_id_t new_root_id = imp_old_root_node->ValueAt(0);
      Page *raw_new_root = buffer_pool_manager_->FetchPage(new_root_id);
      InternalPage *new_root = reinterpret_cast<InternalPage *>(raw_new_root->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->DeletePage(imp_old_root_node->GetPageId());
      buffer_pool_manager_->UnpinPage(new_root_id, true);
    } else {
      // 第二种情况：仅有一个节点了
      root_page_id_ = INVALID_PAGE_ID;
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
      return true;
    }
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  Page *leaf_page = FindLeafPage(KeyType(), nullptr, 0, true);
  // LeafPage *start_node = reinterpret_cast<LeafPage *>(leaf_page);
  return INDEXITERATOR_TYPE(leaf_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf_page = FindLeafPage(key, nullptr, 0, false);
  LeafPage *tmp_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int idx = tmp_node->KeyIndex(key, comparator_);
  // LeafPage *start_node = reinterpret_cast<LeafPage *>(leaf_page);
  return INDEXITERATOR_TYPE(leaf_page, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

// 并发工具
// mode:
// 0 - read
// 1 - insert
// 2 - delete
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseTxnPage(Transaction *txn, int mode){
  std::shared_ptr<std::deque<Page *>> dqueue_page = txn->GetPageSet();
  while (!dqueue_page->empty()){
    Page* ptr = dqueue_page->front();
    dqueue_page->pop_front();
    page_id_t p_id = ptr->GetPageId();
    if(mode == 0){
      ptr->RUnlatch();
    } else {
      ptr->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(p_id, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::CheckSafe(BPlusTreePage* page_ptr, int mode){
  assert(mode >= 0 && mode <= 2);
  if (mode == 0) {
    // read
    return true;
  } else if (mode == 1) {
    // insert
    if(page_ptr->GetSize() < page_ptr->GetMaxSize() - 1){
      return true;
    }
    return false;
  } else {
    // delete
    bool is_root = page_ptr->IsRootPage();
    // bool is_leaf = page_ptr->IsLeafPage();
    if(is_root && page_ptr->GetSize() >= 2){
      // only root node
      return true;
    } else if(!is_root && page_ptr->GetSize() > page_ptr->GetMinSize()){
      // 
      return true;
    }
    return false;
  }
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * leftmost 是为了index迭代器使用的
 */

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *txn, int mode, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  // Page *get_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // if (txn != nullptr) {
  //   deque_page = txn->GetPageSet();
  // }
  page_id_t p_id = root_page_id_;
  BPlusTreePage *sub_page;
  Page *raw_sub_page;
  while (true) {
    raw_sub_page = buffer_pool_manager_->FetchPage(p_id);
    
    // 事务并发使用
    if(txn != nullptr){
      if(mode == 0){
        raw_sub_page->RLatch();
      } else {
        raw_sub_page->WLatch();
      }
    }
    sub_page = reinterpret_cast<BPlusTreePage *>(raw_sub_page->GetData());
    if(txn != nullptr){
      if(CheckSafe(sub_page, mode)){
        ReleaseTxnPage(txn, mode);
      }
      txn->AddIntoPageSet((reinterpret_cast<Page *>(sub_page)));
    }
    
    if(sub_page->IsLeafPage()){
      break;
    }
    InternalPage *search_page = reinterpret_cast<InternalPage *>(sub_page);
    if (leftMost) {
      // 为了index迭代器使用
      p_id = search_page->ValueAt(0);
    } else {
      // 正常的二分查找
      p_id = search_page->Lookup(key, comparator_);
    }
    if(mode == 0 || txn == nullptr){
      buffer_pool_manager_->UnpinPage(search_page->GetPageId(), false);
    }
  }
  Page *ans = reinterpret_cast<Page *>(sub_page);
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *txn){
  const auto pages = txn->GetDeletedPageSet();
  for(const auto &p : *pages){
    buffer_pool_manager_->DeletePage(p);
  }
  pages->clear();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

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

#include <algorithm>
#include <sstream>

#include "common/config.h"
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
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetMaxSize(max_size);
}

/**
 * Insert: 插入一个kv到page中, 返回插入后page中的kv数量
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator)
    -> int {
  // auto index = this->KeyIndex(key, keyComparator);
  // // 1. 如果key已经存在, 直接返回
  // if(keyComparator(this->array_[index].first, key) == 0) {
  //   return this->GetSize();
  // }
  // // 2. 否则插入对应的位置, 先通过move_backward统一向后移动一位, 空出的位置放新的kv;
  // std::move_backward(array_+index, array_ +GetSize(),array_+GetSize()+1);
  // this->SetArrayPage(index, key, value);
  // this->IncreaseSize(1);
  // return this->GetSize();
  auto distance_in_array = KeyIndex(key, keyComparator);
  if (distance_in_array == GetSize()) {
    *(array_ + distance_in_array) = {key, value};
    IncreaseSize(1);
    return GetSize();
  }

  if (keyComparator(array_[distance_in_array].first, key) == 0) {
    return GetSize();
  }

  std::move_backward(array_ + distance_in_array, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_ + distance_in_array) = {key, value};

  IncreaseSize(1);
  return GetSize();
}

/*`
  * Loopup: 查找key所在的节点
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const
    -> bool {
  int index = KeyIndex(key, keyComparator);
  if (index == GetSize() || keyComparator(array_[index].first, key) != 0) {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
    return false;
  }
  *value = array_[index].second;
  return true;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return this->next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return this->array_[index].first;
}
/*
 * KeyIndex: 用于插入或者查找是否在页中，定位应在的index
 * 本质上就是在array中二分查找key应该出现的位置:
 * 如果存在key, 返回key的下标; 不存在key则返回key应插入的位置
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto item = std::lower_bound(array_, array_ + GetSize(), key,
                               [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  // 此时就是item.first == key的位置
  return std::distance(array_, item);
}
/**
 * Helper methods to set/get array
 * set/get this->array的方法, 通过index访问
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArrayPage(int index) const -> MappingType{
  return *(this->array_+index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetArrayPage(int index, const KeyType &key, const ValueType &value){
  *(this->array_+index) = {key,value};
}

/**
 * Helper methods to set/get array item
 * set/get this->array[index]的方法, 通过index访问
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const std::pair<KeyType, ValueType> & {
    return array_[index];
}

/*
  * MoveHalfTo: 将node的一半挪到recipient node中
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  int split_start_index = GetMinSize();
  int size = GetSize() - split_start_index;
  SetSize(split_start_index);
  recipient->CopyNFrom(array_ + split_start_index, size);
}

/*
  * MoveFirstToEndof: 将node的first移动到recipient node的last
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  auto source_first_item = GetItem(0);
  std::move(array_+1,array_+GetSize(),array_);
  IncreaseSize(-1);
  recipient->CopyLastFrom(source_first_item);
}
/*
  * MoveLastToEndof: 将node的last移动到recipient node的first
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  auto source_last_item = array_[GetSize()-1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(source_last_item);
}
/*
  * MoveLastToEndof: 将node的kv全部移到其他page里
*/
/*
 * -> 与internal不同的是，这里K与V是一一对应的，第一个Key也不为空
 * -> 多了一个指向下一个叶list(页)的值:next page id
 * 由于是本页的KV加到其他页后，所以其他页的next_page_id要更新为本页的！
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
    recipient->CopyNFrom(array_,GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}





/*
  * RemoveAndDeleteRecord: leaf node中删除key的record
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
    int index = KeyIndex(key,keyComparator);
    if(index == GetSize() || keyComparator(array_[index].first,key) != 0){
      return GetSize();
    }
    std::move(array_+index+1,array_+GetSize(),array_+index);
    IncreaseSize(-1);
    return GetSize();
}

//================================private的内部方法=====================================//
/*
  * CopyNFrom: 从array中拷贝size个pair,
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(std::pair<KeyType, ValueType> *items, int size) {
    std::copy(items,items+size,array_+GetSize());
    IncreaseSize(size);
}
/*
  * CopyFirstFrom: 在node->array的first插入一个item
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &item) {
  std::move_backward(array_,array_+ GetSize(),array_+GetSize()+1);
  *array_ = item;
  IncreaseSize(1);
}
/*
  * CopyLastFrom: 在node->array的last插入一个item
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const std::pair<KeyType, ValueType> &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

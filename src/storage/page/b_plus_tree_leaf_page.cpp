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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator) -> int {
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
  auto index = KeyIndex(key, keyComparator);
  // 2.
  if (index == GetSize()) {
    *(array_ + index) = {key, value};
    IncreaseSize(1);
    return GetSize();
  }
  // 1.
  if (keyComparator(array_[index].first, key) == 0) {
    return GetSize();
  }
  // 3.
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_ + index) = {key, value};
  IncreaseSize(1);
  return GetSize();
}

/*
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

/*
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  int split_start_index = GetMinSize();
  int size = GetSize() - split_start_index;
  SetSize(split_start_index);
  recipient->CopyNFrom(array_ + split_start_index, size);
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

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

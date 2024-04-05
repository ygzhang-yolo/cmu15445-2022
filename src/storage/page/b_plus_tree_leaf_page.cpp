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
/*
 * 与internal不同的是，这里K与V是一一对应的，第一个Key也不为空
 * 多了一个在本页KV对最后，指向下一个叶list(页)的值:next page id,初始设置为INVALID_PAGE_ID
 * 如果向后加入新的，则next page id应该改为新的指向的id
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    // 与internal页相比的区别，叶的value就是这个key所代表的数据的内存地址！
    SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {next_page_id_ = next_page_id;}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
    return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const std::pair<KeyType, ValueType> & {
    return array_[index];
}

/*
 * 用于插入或者查找是否在页中，定位应在的index
 * 类似internal页中的Lookup, 找到这个key所在的KV对的index，或者是找到这个要插入的key应该在的index
 * 区别在于这个不存在区间，要找到第一个不小于key的位置的index( ==key，找到；> key 要插入)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
    auto item = std::lower_bound(array_,array_+ GetSize(),key,[&comparator](const auto &pair,auto k){
      return comparator(pair.first, k) < 0;
    });
    // 此时就是item.first == key的位置
    return std::distance(array_,item);
}

/*
 * 插入叶<key,value>与页中
 * 1. key存在，返回
 * 2. 要插入尾端、
 * 3. 要插入中间
 * */
INDEX_TEMPLATE_ARGUMENTS
  auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator) -> int {
    auto index = KeyIndex(key,keyComparator);
    // 2.
    if(index == GetSize()){
      *(array_+index) = {key,value};
      IncreaseSize(1);
      return GetSize();
    }
    // 1.
    if(keyComparator(array_[index].first,key) == 0){
      return GetSize();
    }
    // 3.
    std::move_backward(array_+index, array_ +GetSize(),array_+GetSize()+1);
    *(array_+index) = {key,value};
    IncreaseSize(1);
    return GetSize();
  }

  /*
   * 用于要超过MaxSize，split，方法同internal页
   * */
INDEX_TEMPLATE_ARGUMENTS
  void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  int splitStartIndex = GetMinSize();
  int size = GetSize() - splitStartIndex;
  SetSize(splitStartIndex);
  recipient->CopyNFrom(array_+splitStartIndex,size);
}

/*
 * 不存在有KV对的key为空，所以可以直接copy
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(std::pair<KeyType, ValueType> *items, int size) {
    std::copy(items,items+size,array_+GetSize());
    IncreaseSize(size);
}

/*
 * 能否查到key为key的节点，返回值是布尔值
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const -> bool {
    int index = KeyIndex(key,keyComparator);
    if(index == GetSize() || keyComparator(array_[index].first,key) != 0){
      return false;
    }
    return true;
}

/*
 * 删除并返回删除后的KV对数量
 * */
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

/*
 * 将本页的KV对全部移到其他页里面
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

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  auto sourceFirstItem = GetItem(0);
  std::move(array_+1,array_+GetSize(),array_);
  IncreaseSize(-1);
  recipient->CopyLastFrom(sourceFirstItem);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const std::pair<KeyType, ValueType> &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) {
  auto sourceLastItem = array_[GetSize()-1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(sourceLastItem);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &item) {
  std::move_backward(array_,array_+ GetSize(),array_+GetSize()+1);
  *array_ = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

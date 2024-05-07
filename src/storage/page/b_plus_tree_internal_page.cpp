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

#include "common/config.h"
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
// Init: 初始化内置参数, 初始化b_plus_tree_page的对应成员即可
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size); 
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return this->array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  this->array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return this->array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * 通过value找到index
 * K：能比较大小的索引  V: page id(这个页中包含的是当前K指向的下一层子树的KV列表)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  // 步骤是遍历array_(0-size)，找到pair.second与value一样的，返回其对应的index(可以用指针作差来表示，
  // 这里使用std::distance函数，能适应迭代器，更具有普适性。作差只适用于数组指针)
  // 那么这种遍历查找就可以用函数模板 find_if来写
  auto it = std::find_if(array_,array_+GetSize(),[&value](const auto &pair){
    return pair.second == value;});
  return std::distance(array_,it);
}


/*
 * Lookup: 在B+树的中间节点中检索key
 * 找到第一个满足pair.first ≥ key的 pair 的value(指向的子树，也就是：下一层中，key≤键值<下一个键值 的KV对存放的页的页id)
 * 注意：内部节点的value存放的子树列表是前一个的范围  前一个k≤ 子树 < 其k
 * key:      空   |  key1  |  key2  |  key3  | ... | array_GetSize()-1.first()
 * value:  ＜key1 |k1≤ ＜k2| k2≤ ＜k3| k3≤ ≤k4| ... | ≥ array_GetSize()-1.first()
 * KV对: <null,V0>| <K1,V1>| <K2,V2>| <K3,V3>| ... | ...
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // 查找第一个不小于key的页位置
  // 默认行为是 !(element<k)时返回，即查找第一个≥k的值
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  // 1. key比此页都大, 返回最右侧指针
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  // 2. key位于页中, 返回对应的指针
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  // 3. key不在此页, 返回前一个指针
  return std::prev(target)->second;
}

/*
 * 将后半段分离出去，加到另一个页中
 * 用于长度超过MaxSize时
 * recipient：需要加进的目的页
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient,
                                                BufferPoolManager *bufferPoolManager) {
  int split_from_index = GetMinSize();
  int split_size = GetSize() - split_from_index;
  SetSize(split_from_index);  // 设置分离后，原页中的size
  recipient->CopyNFrom(array_ + split_from_index, split_size, bufferPoolManager);
  // int start_split_indx = GetMinSize();
  // int original_size = GetSize();
  // SetSize(start_split_indx);
  // recipient->CopyNFrom(array_ + start_split_indx, original_size - start_split_indx, bufferPoolManager);
}
/*
 * 生成新的根节点叶面
 * 用于根节点页面分裂，此时要开辟一个新的页作为新root，新root中有两个KV对(1个key)
 * K  null | key1(newKey)
 * V  oldV | newV
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &oldValue, const KeyType &newKey, const ValueType &newValue) {
  SetKeyAt(1,newKey);
  SetValueAt(0,oldValue);
  SetValueAt(1,newValue);
  SetSize(2);
}

/*
 * 向oldValue后插入<newKey,newValue>,并返回插入后的size
 * 直接插入，不考虑需要split或合并等的问题，类似于在数组中插入一个元素
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &oldValue, const KeyType &newKey, const ValueType &newValue) -> int {
  /*
   * 1. 获得要插入的id 2. 将后面的元素统一后移 3. 插入并将size++
   * */
  auto new_value_index = ValueIndex(oldValue) + 1;
  std::move_backward(array_+ new_value_index,array_+GetSize(),array_+GetSize()+1);
  array_[new_value_index].first = newKey;
  array_[new_value_index].second = newValue;

  IncreaseSize(1);
  return GetSize();
}

//============================private: 内部辅助函数============================//
/*
 * 一般用于将某一段(可能是长度不够MinSize 可能是长度超过MaxSize)复制到另一个internal的页中
 * 某段： items~items+size-1
 * 1. 将给定的某一段复制到当前internal页(待合入的目的页)的array_中
 * 2. 某一段中的每个KV对中的value都是这个KV对中的K指向的其下一层的子树列表的pageId
 *    也就是说，每一个value对应的页，其parent page id仍是这个KV对所在的源page id
 *    我们已经将这个KV对迁移过来了，还不要忘记更新V对应的页的Parent page id为当前internal页 id
 * 3. 更新目的页中含有的KV对数目
 * 注意：我们更新页是在Buffer pool上更新，更新后置为脏页，不立即写回磁盘捏！
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(std::pair<KeyType, ValueType> *items, int size, BufferPoolManager *bufferPoolManager) {
  std::copy(items, items+size, array_+GetSize());
  // 2.
  for(int i = 0; i < size; ++i){
    // 获得V对应的页
    auto page = bufferPoolManager->FetchPage((items+i)->second); // 当然也可以是ValueIndex(i + GetSize())
    // 将其data转为BPlusTreePage类型，才能设置parent page id
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());-
    node->SetParentPageId(GetPageId());
    // 解除固定，告诉Buffer pool现在是脏页(不立即写回)
    bufferPoolManager->UnpinPage(page->GetPageId(),true);
  }
  IncreaseSize(size);
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

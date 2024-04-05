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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    // 按需求设置参数，父类
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0); // 页内kv对数量
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // 获得下标为index的kv对中的key(连续的柔性数组)
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key;}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

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
  auto item = std::find_if(array_,array_+GetSize(),[&value](const auto &pair){
    return pair.second == value;});
  return std::distance(array_,item);
}

/*
 * 在B+数内部页面中查找key所在的子树，注意此时不是精确的key == key
 * 而是找到第一个 pair.first ≥ key的 pair 的value(指向的子树，也就是：下一层中，key≤键值<下一个键值 的KV对存放的页的页id)
 * 因为在这个B+树的设计中，内部节点的value存放的子树列表是前一个的范围  前一个k≤ 子树 < 其k
 * 我们需要的就是这个子树的页id的首节点
 * 这里要注意的是 第一个位置不存放k  即n个value只需要n-1个key
 * key:      空   |  key1  |  key2  | key3 |
 * value:           ＜key1 |k1≤ ＜k2| k2≤ ＜k3| ≥k3
 * 获取方式:           key1   key2     key3     array_[GetSize()-1].second()
 * */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // 查找第一个不小于key的页位置
  auto item = std::lower_bound(array_+1 , array_+GetSize(),key,[&comparator](const auto &pair, auto k){
    return comparator(pair.first,k) < 0; // 默认行为是 !(element<k)时返回，即查找第一个≥k的值
  });
  if(item == array_+GetSize()){
    // 在end,key大于所有的k,应该对应最后一段value
    return ValueAt(GetSize()-1);
  }
  if(comparator(item->first,key) == 0){
    // 相等，则就是要的这段
    return item->second;
  }
  // item.first > key,key在其前一个管的子树中
  return std::prev(item)->second;
}

/*
 * 将一个新的key置为root
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
  auto newValueIndex = ValueIndex(oldValue) + 1;
  std::move_backward(array_+ newValueIndex,array_+GetSize(),array_+GetSize()+1);
  array_[newValueIndex].first = newKey;
  array_[newValueIndex].second = newValue;

  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType,ValueType, KeyComparator> *recipient, BufferPoolManager *bufferPoolManager) {
  int splitFromIndex = GetMinSize();
  int
}

/*
 * 一般用于将某一段(可能是长度不够MinSize 可能是长度超过MaxSize)复制到另一个internal的页中
 * 1. 将给定的某一段复制到当前internal页(待合入的目的页)的array_中
 * 2. 某一段中的每个KV对中的value都是这个KV对中的K指向的其下一层的子树列表的pageId
 *    也就是说，每一个value对应的页，其parent page id仍是这个KV对所在的源page id
 *    我们已经将这个KV对迁移过来了，还不要忘记更新V对应的页的Parent page id为当前internal页 id
 * 注意：我们更新页是在Buffer pool上更新，更新后置为脏页，不立即写回磁盘捏！
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(std::pair<KeyType, ValueType> *items, int size, BufferPoolManager *bufferPoolManager) {
  std::copy(items, items+size, array_+size);
  // 2.
  for(int i = 0; i < size; ++i){
    // 获得V对应的页
    auto page = bufferPoolManager->FetchPage((items+i)->second); // 当然也可以是ValueIndex(i + GetSize())
    // 将其data转为BPlusTreePage类型，才能设置parent page id
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    // 解除固定，告诉Buffer pool现在是脏页(不立即写回)
    bufferPoolManager->UnpinPage(page->GetPageId(),true);
  }
}



// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

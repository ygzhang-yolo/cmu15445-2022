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
 * key:      空   |  key1  |  key2  |  key3  | ... | array_GetSize()-1.first()
 * value:  ＜key1 |k1≤ ＜k2| k2≤ ＜k3| k3≤ ≤k4| ... | ≥ array_GetSize()-1.first()
 * KV对: <null,V0>| <K1,V1>| <K2,V2>| <K3,V3>| ... | ...
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

/*
 * 将后半段分离出去，加到另一个页中
 * 用于长度超过MaxSize时
 * recipient：需要加进的目的页
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType,ValueType, KeyComparator> *recipient, BufferPoolManager *bufferPoolManager) {
  int splitFromIndex = GetMinSize();
  int splitSize = GetSize() - splitFromIndex;
  SetSize(splitFromIndex); // 设置分离后，原页中的size
  recipient->CopyNFrom(array_+ splitFromIndex, splitSize,bufferPoolManager);
}

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
  IncreaseSize(size);
}

/*
 * 删除页KV对列表中，索引为index位置的KV对
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::move(array_+index+1,array_ + GetSize(),array_+index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  ValueType onlyValue = ValueAt(0);
  SetSize(0);
  return onlyValue;
}

/*
 * 将此时页中的所有KV对移到另一个页中
 * 用于size < MinSize 时，合并入其他页
 * 将自己整个迁移过去，而原本自己的第一个KV对是<null,V0>，所以
 * 1. 要将自己第一个K0补上，也就是应该是目标页的最后一个K(如果不超MaxSize)；
 * 2. 最后迁移过去后，
 * 3. 不要忘记将源页size设置为0
 * recipient：其他页  middleKey：目标页的最后一个K
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient, const KeyType &middleKey, BufferPoolManager *bufferPoolManager) {
  SetKeyAt(0,middleKey);
  recipient->CopyNFrom(array_,GetSize(),bufferPoolManager);
  SetSize(0);
}

/*
 * 将第一个KV对(其中K用middleKey赋值)移到另一个页后
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient, const KeyType &middleKey, BufferPoolManager *bufferPoolManager) {
  SetKeyAt(0,middleKey);
  auto sourceFirstPair = array_[0];
  recipient->CopyLastFrom(sourceFirstPair,bufferPoolManager);

  std::move(array_+1,array_+GetSize(),array_); // 删除KV对中的第一个(删除数组第一个)
  IncreaseSize(-1);
}

/*
 * 将给定的一个pair添加到当前页最后(步骤同CopyNFrom)
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const std::pair<KeyType, ValueType> &pair, BufferPoolManager *bufferPoolManager) {
  *(array_ + GetSize()) = pair;
  IncreaseSize(1);

  auto page = bufferPoolManager->FetchPage(pair.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  bufferPoolManager->UnpinPage(page->GetPageId(),true);
}

/*
 * 将最后一个KV对(其中K用middleKey赋值)移到另一个页的KV list最开头
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient, const KeyType &middleKey, BufferPoolManager *bufferPoolManager) {
  auto lastItem = array_[GetSize() - 1];
  recipient->SetKeyAt(0,middleKey);
  recipient->CopyFirstFrom(lastItem,bufferPoolManager);

  IncreaseSize(-1);
}

/*
 * 将pari插入页的KV对的开头
 * */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &pair, BufferPoolManager *bufferPoolManager) {
  std::move_backward(array_,array_+GetSize(),array_+GetSize()+1);
  *array_ = pair;
  IncreaseSize(1);

  auto page = bufferPoolManager->FetchPage(pair.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  bufferPoolManager->UnpinPage(page->GetPageId(),true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index)
    : buffer_pool_manager_(bpm), page_(page), index_(index) {
  if (page != nullptr) {
    leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
  } else {
    leaf_ = nullptr;
  }
}
/*
 * ~IndexIterator: 析构函数来unpinned page
*/
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}
/*
 * IsEnd: 判断迭代器是否是叶子节点的最后一个
*/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}
/*
 * *Iterator: 返回叶子节点中index对应的kv的引用
*/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetItem(index_); }
/*
 * Iterator++: 返回当前index对应的下一个kv
 * 1. 若当前KV对在当前页后面还有，则不需要切换Page，直接index++即可
 * 2. 若index已经到当前叶节点页KV对的最后了，则leaf_切换到下一个挨着的右边叶节点页，index为0
 * 3. 若是2的情况，且已无右边叶节点页，则直接index++(相当于到iterator.end(),在取值的时候会判断是否有意义~)
*/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ == leaf_->GetSize() - 1 && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    // 1. index为当前page的末尾, 但还有下一个page, 切换到下一个page的第一个
    auto next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);

    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}
/*
 * 迭代器相等: 1.叶子节点的page id相同; 2.index相同
*/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return leaf_ == nullptr || (leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_);
}
/*
 * 迭代器不等: ==的相反
*/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

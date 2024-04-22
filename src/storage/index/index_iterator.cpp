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
/*
 * 初始化：bpm、page、index
 * 将page_强转给leaf_初始化
 * */
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
 * 析构时要删除delete new出来的(无)；并释放相应锁；UnPin相应页面
 * */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
 if (page_ != nullptr) {
   page_->RUnlatch(); // 只是查找，上的读锁，释放读锁，下面的UnPin也是false
   buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
 }
}

/*
 * 判断是不是到了最后一个叶节点页(最右边的)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
 return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}

/*
 * 重载取值运算符：*index_iterator 代表返回当前叶节点页内下标为index的KV对的引用(MappingType &)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetItem(index_); }

/*
 * 前置++：返回的是当前index代表的KV对的下一个KV对的index与leaf_：
 * 1. 若当前KV对在当前页后面还有，则不需要切换Page，直接index++即可
 * 2. 若index已经到当前叶节点页KV对的最后了，则leaf_切换到下一个挨着的右边叶节点页，index为0
 * 3. 若是2的情况，且已无右边叶节点页，则直接index++(相当于到iterator.end(),在取值的时候会判断是否有意义~)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
 if (index_ == leaf_->GetSize() - 1 && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
   auto next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());

   next_page->RLatch();
   page_->RUnlatch();
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
 * 两个迭代器相等指的是: 1. 当前迭代器指向的叶节点页id与其指向的相同 2. 在叶节点页中index相同即可
 * */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
 return leaf_ == nullptr || (leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_);
}

/*
 * ==反过来
 * */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return this->root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf_page = InterFindLeaf(key, Operation::SEARCH, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType v;
  auto existed = node->Lookup(key, &v, comparator_);

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  if (!existed) {
    // 没找到key对应的value值
    return false;
  }
  result->push_back(v);
  return true;
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1. 如果tree是空的, start new tree
  if(this->IsEmpty()) {
    this->InterStartNewTree(key, value);
    return true;
  }
  return InterInsertLeaf(key, value, transaction);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return this->root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
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
    auto *inner = reinterpret_cast<InternalPage *>(page);
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
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
//===========================内部功能函数=============================//
/*
 * InterStartNewTree: 创建一个B+树, 并将kv插入
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InterStartNewTree(const KeyType &key, const ValueType &value) {
  auto page = this->buffer_pool_manager_->NewPage(&this->root_page_id_);
  // 1. Buffer pool中无法new新的page，报错
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  // 2. 创建一个leaf node, 并调用insert方法, 将kv数据插入到叶子节点中
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_node->Init(this->root_page_id_, INVALID_PAGE_ID, this->leaf_max_size_);
  leaf_node->Insert(key, value, this->comparator_);
  // 3. page用完后, unpin bp
  this->buffer_pool_manager_->UnpinPage(root_page_id_, true);
}
/*
  * InterInsertLeaf: 插入叶子节点
*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InterInsertLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool{
  // 1. FindLeaf, 找到要插入的Page
  auto leaf_page = InterFindLeaf(key, Operation::INSERT, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 2. 插入数据, 判断是否满了, 进行split
  auto size = node->GetSize();
  auto new_size = node->Insert(key, value, comparator_);
  // 3.1 重复插入, 直接return 
  if(size == new_size) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  // 3.2 不需要split, 直接插入
  if(new_size < this->leaf_max_size_) {
    this->buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true); 
    return true;
  }
  // 3.3 需要触发split
  auto split_new_leaf_node = Split(node);
  split_new_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(split_new_leaf_node->GetPageId());
  // 同时下层分裂后，要将新分裂的页的第一个key提到上层中插入
  auto risen_key = split_new_leaf_node->KeyAt(0);
  this->InterInsertParent(node, risen_key, split_new_leaf_node, transaction);
  // 4. 释放page, return
  this->buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(split_new_leaf_node->GetPageId(), true);
  return true;
}
/* 
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InterInsertParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                       Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // 根节点分裂，需要创建新页为根节点页
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (root_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
    new_root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_root_node->GetPageId());
    new_node->SetParentPageId(new_root_node->GetPageId());

    buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);

    UpdateRootPageId(0);  // 这个函数的意思是更新(0)/插入(1)根节点页的ID,因为此时已经有根节点页ID了，只需要更新，不需要插入
    return;
  }
  // 非根节点分裂,需要将new_node的第一个key(在这里即传入的参数key)提给父节点(要注意判断父节点页中Size数量，需要split咩)
  // 获取父节点页与Node
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (parent_node->GetSize() < internal_max_size_) {
    // 还可以至少再插入一个而不split，直接插入
    // 插入后就安全了，逐级释放锁，然后UnPin
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // 提到父节点页，父节点页满了需要splitQAQ
  /*
   * 1. 首先，分配一定的内存mem来临时存储父节点的内容和额外的一个键值对；并将父节点当前完整内容复制到新分配的内存中
   * 2. 插入新的键值对(正好占满mem)，然后在mem转化的node上调用split分裂，同时返回一个存放剩余键值对的新父页
   * 3. 父页分裂所以也要把分裂后的KV上提给父父页，需要存一下
   * 4. 同时将分裂后的应属于原父节点的内容再写回原父节点
   * 5. 然后递归调用当前函数，将父页分裂后的新KV对存入父父页
   * 6. 最后解除固定，清理内存(对于锁的释放早在递归调用中就已经完成啦~)
   * */
  // 1.
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)];
  auto *copy_old_parent_node = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, parent_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * parent_node->GetSize());
  // 2.
  copy_old_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  auto new_split_parent_node = Split(copy_old_parent_node);
  // 3.
  KeyType new_parent_key = new_split_parent_node->KeyAt(0);
  // 4.
  std::memcpy(parent_page->GetData(), mem,
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_old_parent_node->GetMinSize());
  // 5.
  InterInsertParent(parent_node, new_parent_key, new_split_parent_node, transaction);
  // 6.
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_split_parent_node->GetPageId(), true);
  delete[] mem;
}

/*
  * Internal FindLeaf: 查找key所在的叶子节点
  * 本质上就是个链表的DFS, 按照key一路查下去即可
*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InterFindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost, bool rightMost) -> Page * {
  assert(root_page_id_ != INVALID_PAGE_ID);
  auto page = this->buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  LOG_DEBUG("here");
  // 遍历到叶子节点找key
  while(!node->IsLeafPage()) {
    // 按照key的有序, 寻找下一层的id
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id = 0;
    if(leftMost) {
      child_page_id = internal_node->ValueAt(0);
    }else if(rightMost) {
      child_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    }else {
      child_page_id = internal_node->Lookup(key, comparator_);
    }
    LOG_DEBUG("child_page_id : %d", child_page_id);
    assert(child_page_id > 0);
    // 切换到下一层, 继续查找
    auto child_page = this->buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if(operation == Operation::SEARCH) {
      this->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    page = child_page;
    node = child_node;
  }
  return page;
}

/*
 * 当Size == MaxSize后，分裂(内部节点与叶节点通用)
 * 调用后，原node只保留前GetMinSize个KV对，返回的新node包含剩余的GetSize-GetMinSize个KV对
 * 1. 先在Buffer pool中开新页，并检查是否开辟成功
 * 2. 利用新页强转为Node，并根据传入的node的type设置新页应该为内部节点的还是叶节点的
 * 3. 再将node与新Node转为对应的Internal 与 Leaf类型，以便调用各自的new的Init函数与MoveHalf函数，移动一半数据去新页
 * 注意：
 *  在初始化新子页时，会新子Page的ParentPageId和旧子页的一样，这样在后面将新子页第一个key与页id作为KV插入父页时，
 *  如果如果父页不会分裂，就不需要再设置ParentPageId了;
 *  叶磁盘页分裂时，要更新叶磁盘页间链表指针，此操作在叶磁盘页调用split函数外的函数里有(本函数无哦)，严谨嘿！
 * */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 1. 从buffer pool中创建一个新page
  page_id_t page_id;
  auto new_page = this->buffer_pool_manager_->NewPage(&page_id);
  if(new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY,"Cannot allocate new newPage");
  }
  // 2. 创建新的node
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());
  // 3.1 对于leaf node, 
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    // 在初始化新子页时，表明新子Page的ParentPageId和旧子页的一样
    new_leaf_node->Init(new_page->GetPageId(), leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
    // 更新叶磁盘页间链表指针的操作在调用split函数外的函数里有，严谨嘿！
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_node);

    // 在初始化新子页时，表明新子Page的ParentPageId和旧子页的一样
    new_internal_node->Init(new_page->GetPageId(), internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }
  return new_node;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

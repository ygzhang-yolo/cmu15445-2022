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
  // 1. 找到key所在的leaf node
  auto leaf_page = InterFindLeaf(key, Operation::SEARCH, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 2. 在leaf中Lookup找到key是否存在
  ValueType v;
  auto existed = node->Lookup(key, &v, comparator_);
  // 3. 处理返回结果, v中就是返回的结果
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
  // 2. 将key插入到对应的叶子节点中
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 1. 空树直接返回
  if (IsEmpty()) {
    return;
  }
  auto leaf_page = InterFindLeaf(key, Operation::DELETE, transaction);    // 找到key所在的leaf
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 2. leaf中不存在此key, 返回
  if (leaf_node->GetSize() == leaf_node->RemoveAndDeleteRecord(key, comparator_)) { //RemoveAndDeleteRecord在leaf中删除对应的key
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  // 3. 要根据删除后的Size检查是否要触发合并
  auto node_should_delete = CoalesceOrRedistribute(leaf_node, transaction);
  // 好了此时这个节点以及删除后的其他节点的调整已经结束
  // 同时循环清除那些放在transaction的deletedPageSet中的页(这些页可能是已经空了，也可能是合并过程中剩下来的不要的页，在最后统一清空一下！)
  if (node_should_delete) {
    transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  // 4. 循环清除需要delete的Page
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&bmp = buffer_pool_manager_](const page_id_t pageId) { bmp->DeletePage(pageId); });
  transaction->GetDeletedPageSet()->clear();
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
 * InterStartNewTree: 创建一个B+树, 并将kv插入到leaf page中
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
  * InterInsertLeaf: 将kv插入叶子节点
  * 1. 将一对kv插入叶子节点, 如果叶子节点满了, 需要进行split操作
  * 2. split会把叶子节点一分为二, 新分出的节点的key[0]，要插入到上层的索引中
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
  // 3.3 需要触发split拆分成两个leaf node: node + split_new_leaf_node
  auto split_new_leaf_node = Split(node);
  // 3.3.1 维护链表next_page的指向, node -> new_leaf_node -> node_next
  split_new_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(split_new_leaf_node->GetPageId());
  // 3.3.2 将新leaf node的第一个key作为索引插入到上层
  auto risen_key = split_new_leaf_node->KeyAt(0);
  this->InterInsertParent(node, risen_key, split_new_leaf_node, transaction);
  // 4. 释放page, return
  this->buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(split_new_leaf_node->GetPageId(), true);
  return true;
}
/* 
  * InterInsertParent(): 将key插入父节点中
  * 一般是发生Split时, 需要把新node的key插入到父节点中
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InterInsertParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                       Transaction *transaction) {
  // 1. 对于根节点的分裂, 需要重建新的根节点指向他俩
  // [1,2,3]->[4,5], 现在根节点裂成了两个, 需要重建一个新的根节点指向二者
  //      [4]
  //    ↙    ↘
  // [1,2,3]  [4,5]
  if (old_node->IsRootPage()) {
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
  // 2. 对于非根节点分裂, 需要将new_node的第一个key插入父节点
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 2.1 若父节点还没满, 直接插入即可
  if (parent_node->GetSize() < internal_max_size_) {
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // 2.2 父节点已满, 插入后又需要split
  // 2.2.1. new出一个mem存储parent node的page data
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)];
  auto *copy_old_parent_node = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, parent_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * parent_node->GetSize());
  // 2.2.2. 插入新的键值对(正好占满mem)，然后在mem转化的node上调用split分裂，同时返回一个存放剩余键值对的新父页
  copy_old_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  auto new_split_parent_node = Split(copy_old_parent_node);
  // 2.2.3. 父页分裂所以也要把分裂后的KV上提给父父页，需要存一下
  KeyType new_parent_key = new_split_parent_node->KeyAt(0);
  // 2.2.4. 将分裂后的应属于原父节点的内容再写回原父节点
  std::memcpy(parent_page->GetData(), mem,
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_old_parent_node->GetMinSize());
  // 2.2.5. 递归调用当前函数，将父页分裂后的新KV对存入父父页
  InterInsertParent(parent_node, new_parent_key, new_split_parent_node, transaction);
  // 2.2.6. unpinned
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
  // 1. 获取根节点所在page
  auto page = this->buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // LOG_DEBUG("here");
  // 2. 遍历所有节点直到leaf page
  while(!node->IsLeafPage()) {
    // 2.1 按照key的有序, 寻找下一层的leaf node
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id = 0;
    // TODO(ubuntu): 这里的leftMost和rightMost是没什么意义的
    // if(leftMost) {
    //   child_page_id = internal_node->ValueAt(0);
    // }else if(rightMost) {
    //   child_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    // }else {
    //   child_page_id = internal_node->Lookup(key, comparator_);
    // }
    child_page_id = internal_node->Lookup(key, comparator_);
    // LOG_DEBUG("child_page_id : %d", child_page_id);
    assert(child_page_id > 0);
    // 2.2 node = next leaf node, 切换到下一层, 继续查找
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
/*
  * Split(): 将node超过min_size的部分, 拷贝到新节点page node中
  * buffer pool中创建新page, 调用MoveHafTo拷贝多余内容到新page node中
*/
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 1. 从buffer pool中创建一个新page
  page_id_t page_id;
  auto new_page = this->buffer_pool_manager_->NewPage(&page_id);
  if(new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY,"Cannot allocate new newPage");
  }
  // 2. 创建新的page node
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());
  // 3.1 对于leaf node;
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    // MoveHalfTo()将超过min_size的部分拷贝到新page node中
    // TIPS: 新Page的ParentPageId和旧子页的一样
    new_leaf_node->Init(new_page->GetPageId(), leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
  } else {
    // 3.2 对于Internal node, 和leaf node操作一样
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_node);

    // 在初始化新子页时，表明新子Page的ParentPageId和旧子页的一样
    new_internal_node->Init(new_page->GetPageId(), internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }
  return new_node;
}

/*
  * CoalesceOrRedistribute: 检查树结构是否发生了改变, 需要重建(改变层数, 发生节点合并)
*/
/*
 * 删除需要删除的KV对后，是否对树的结构进行了改变重建(改变层数、或者发生节点合并为true)
 * 返回值：树结构改变了，true；反之 false
 * 1. 删除后数量没低于最小值，不需要合并，好耶！
 * 2. 是根节点的删除，调整完后就可以返回了(调用的是AdjustRoot函数判断)
 * 3. 非根节点的删除，且节点删除后需要进行合并或者更改结构
 *  3.1 先获得当前删除节点的叶在父节点中的index，判断当前节点页在父节点页的哪个位置：
 *    3.1.1 在非最左边页，则要选择相邻左边页进行借KV对(最好咯)或合并
 *      3.1.1.1 挨着的左边页可以直接匀过来一个KV对！！么么：
 *        调用Redistribute函数；然后释放前面的锁；将父节点页UnPin(锁已释放)；将当前节点锁先释放，然后UnPin 匀给自己KV对的页(自己在Remove页面最后释放锁+UnPin捏)
 *      3.1.1.2 需要合并两个页：
 *        调用Coalesce函数；合并相邻+自己，同时将父节点页上面我们俩的key合成一个，其实就是删除被合并进去的那个key就好啦~
 *        Coalesce在合并完子节点页，执行完父节点页相应KV删除后，会递归调用当前函数，直至所有删除结束！
 *        返回true
 *    3.1.2 在最左边页，选择的只能是相邻右边页，其余逻辑同上
 *
 * 注意：
 * 在过程中会将一些已经空掉的Page或者合并掉后废弃的Page放在transaction的deletePageSet中，等到最后在Remove函数最后遍历删除
 * 锁的释放顺序：先释放前面的(调用R函数)，在释放现在的锁，再UnPin
 * */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // 1. 删除后数量没低于最小值，不需要合并
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  // 2. 对于根节点, 需要删除就调整
  if (node->IsRootPage()) {
    // 是根节点的删除
    auto root_should_del = AdjustRoot(node);
    // 调整完毕，可以直接返回了
    return root_should_del;
  }
  // 3. 对于非根节点，且节点删除后需要进行合并或者更改结构
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto c_index = parent_node->ValueIndex(node->GetPageId());  // leaf所在的internal page的指针index

  if (c_index > 0) {
    // 3.1. page左边存在其他page, 尝试借一个kv
    auto left_c_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(c_index - 1));
    N *left_c_node = reinterpret_cast<N *>(left_c_page->GetData());
    // 3.1.1 左边数量大于min_size, 可以借一个kv
    if (left_c_node->GetSize() > left_c_node->GetMinSize()) {
      Redistribute(left_c_node, node, parent_node, c_index, true);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_c_node->GetPageId(), true);
      return false;
    }
    // 3.1.2 左边不能借, 合并page和page->left
    // 合并相邻+自己，同时将父节点页上面我们俩的key合成一个，其实就是删除被合并进去的那个key就好啦~
    // Coalesce在合并完子节点页，执行完父节点页相应KV删除后，会递归调用当前函数，直至所有删除结束！
    auto parent_node_should_del = Coalesce(left_c_node, node, parent_node, c_index, transaction);
    // 3.1.3 将需要删除的page放入transaction等待删除
    if (parent_node_should_del) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(left_c_node->GetPageId(), true);
    return true;
  }
  // TODO(ubuntu): 感觉下面应该是 == 0，也就是在最左边
  if (c_index != parent_node->GetSize() - 1) {
    // 3.2 page已经是最左边，只能选择右边相邻节点页借，剩下的步骤同3.1
    auto right_c_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(c_index + 1));
    N *right_c_node = reinterpret_cast<N *>(right_c_page->GetData());

    if (right_c_node->GetSize() > right_c_node->GetMinSize()) {
      Redistribute(right_c_node, node, parent_node, c_index, false);

      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_c_node->GetPageId(), true);
      return false;
    }
    // NOTE:调用合并函数时小心，因为第一个为左边页，第二个为右边页
    auto need_remove_index = parent_node->ValueIndex(right_c_node->GetPageId());
    auto parent_node_should_del = Coalesce(node, right_c_node, parent_node, need_remove_index, transaction);
    transaction->AddIntoDeletedPageSet(right_c_node->GetPageId());
    if (parent_node_should_del) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_c_node->GetPageId(), true);
    return true;
  }
  return false;
}

/*
  * AdjustRoot: root删除元素后, 判断是否会导致B+树结构变化
*/
/*
 * 在对于根节点删除元素后，判断删除后是否导致B+树结构的变化(并改变相应变化)，返回值 变化：true，反之false
 * 调整B+树的根节点，在删除操作导致树结构变化后确保树保持正确的形态，两种情况：
 * 1. 当根节点非叶节点且只有一个子节点 -> 即key(根)->key(子)，那么这一层没有存在的必要，可以直接将根移到下一层。
 *  1.1 获得子节点page/node后，将唯一子节点的ParentPageId设置为INVALID_PAGE_ID，因为它现在是根节点。
 *  1.2 更新树的root_page_id_为这个唯一子节点的页面ID
 *  1.3 通过调用UpdateRootPageId(0)更新头部页面中存储的新
 * 2. 根节点是叶节点，且无元素了，代表删除了B+树中的最后一个元素 -> 设置无效直接返回true
 * */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node) -> bool {
  // 1. 当根节点非叶节点且只有一个子节点 -> 即key(根)->key(子)，那么这一层没有存在的必要，可以直接将根移到下一层。
  if (!node->IsLeafPage() && node->GetSize() == 1) {
    auto *root_node = reinterpret_cast<InternalPage *>(node);
    auto only_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
    auto *only_child_node = reinterpret_cast<BPlusTreePage *>(only_child_page->GetData());
    only_child_node->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = only_child_node->GetPageId();

    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);
    return true;
  }
  // 2. 根节点是叶节点，且无元素了，代表删除了B+树中的最后一个元素
  if (node->IsLeafPage() && node->GetSize() == 0) {
    
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  // 3. 不需要调整树root的结构
  return false;
}

/*
  * Redistribute: 待删除node，向临近Node借一个kv
*/
/*
 * 这个就是用来，删除节点的节点页，向附近节点页借1个KV对的操作
 * 当然附近节点页要自身能借(size>MinSize)
 * 分叶节点页和内部节点页两种(逻辑一样，只是内部节点页的第一个Key需要从父节点对应位置的key中获取)
 * 1. 向右边节点借：实际上是将右边节点页的第一个移到当前节点页的最后
 *    由于父节点中对应位置的key是各个子节点页的第一个key(叶节点页，有值)，value是各个子节点页的page_id
 *    所以还要更新右边节点页在父节点页中对应位置的key值
 * 2. 向左边的节点页借1：实际上是将左边节点页的最后一个移到当前节点页的开头
 *    其余逻辑同
 *
 * 参数：
 * index：node在Parent节点页中的KV下标
 * from_prev： true -> neighbor是相邻左边节点页；false -> 是相邻右边节点页
 * */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index, bool from_prev) {
  if(node->IsLeafPage()){
    // 1. leaf node
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if(!from_prev){
        // 1.1 向右边的节点页借1：实际上是将右边节点页的第一个移到当前节点页的最后
        // 由于父节点中对应位置的key是各个子节点页的第一个key(叶节点页，有值)，value是各个子节点页的page_id
        // 所以还要更新右边节点页在父节点页中对应位置的key值
        neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
        parent->SetKeyAt(index+1,neighbor_leaf_node->KeyAt(0));
    } else {
        // 1.2 同理，向左边的节点页借1：实际上是将左边节点页的最后一个移到当前节点页的开头
        neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
        parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    // 2. internal node
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if(!from_prev){
        // 2.1 向右边的节点页借1：实际上是将右边节点页的第一个移到当前节点页的最后
        // 与叶节点页不同的是，内部节点页的第一个key为null，实际保存在上层的parent中(也就是上层自己对应位置的key值)，所以要作为参数传入
        // 其余与上同
        neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index+1),buffer_pool_manager_);
        // 这里有个小trick，原本新移动后的neighborInternalNode的第一个key也是空，但这里没有人为设置为空，只是会避免使用，所以可能更好的方式是在移动前，存放一下Key2 P
        parent->SetKeyAt(index+1,neighbor_internal_node->KeyAt(0));
    } else {
        // 2.2 同理，向左边的节点页借1：实际上是将左边节点页的最后一个移到当前节点页的开头
        neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index),buffer_pool_manager_);
        parent->SetKeyAt(index,internal_node->KeyAt(0));
    }
  }
}

/*
  * Coalesce: 合并两个子节点页，返回最后是否对树的结构进行了改变重建，即层数发生了改变
*/
/*
 * 合并：
 * 1. 将node中的KV对加入到neighbor_node的kV对后
 * 2. 同时删除在Parent中的index位置的key,v对(因为这个指针指向的叶被合并起来啦，还要加入transaction的deletePageSet，最后统一删除捏)
 * 3. 因为父节点也删除节点了，所以要将父节点作为node，重新递归调用一遍CoalesceOrRedistribute判断哦！
 * */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index, Transaction *transaction) -> bool {
  auto middle_key = parent->KeyAt(index); // 父节点页中因为合并需要删除的key
  // 1. leaf
  if(node->IsLeafPage()){
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *prev_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    leaf_node->MoveAllTo(prev_leaf_node);
  } else {
    // 2. internal page
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *prev_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    internal_node->MoveAllTo(prev_internal_node,middle_key,buffer_pool_manager_);
  }
  // 3. 删除parent中对应的key
  parent->Remove(index); // 删除合并在里面的那个key
  return CoalesceOrRedistribute(parent,transaction); // 因为父节点也删除节点了，所以要将父节点作为node，重新递归调用一遍判断
}


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

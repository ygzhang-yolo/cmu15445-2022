#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
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
 * 一旦插入 root_page_id不会非法
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }


/*
 * FindLeaf函数，默认leftMost与rightMost是false(这两个其实是在已知在最左边或者最右边的情况下，加快查询效率)
 * 找到符合条件的leaf(也可能是要删除或插入的位置)所在页！
 * 1. 先获得根节点页 与 根节点
 * 2. 对于不同操作上不同程度的锁：
 *  2.1 搜索操作：根节点去掉读锁，page上读锁
 *  2.2 插入操作与删除操作对于page上写锁，其中为了优化并发性能、减少死锁可能性，维持操作的局部性，我们要对一些绝对安全的插入删除情况释放一些锁
 *      绝对安全指的是：插入操作时，当前就算插入也不会==MaxSize 而split；删除操作时，当前就算删除也不会小于MinSize 而合并
 *    2.2.1 删除时，由于当前是根节点，只要当前Size > 2即可
 *    2.2.2 插入时，如果当前是非叶节点，要求Size < MaxSize；如果当前是叶节点，要求Size < MaxSize - 1(叶节点的意思就是只有一层咯害)
 * 3. 然后循环node向下遍历，按照规定找到下一层的页id，获得下一层的cPage与cNode，直至到叶节点层，其中也要分操作按规定释放锁：
 *  3.1 搜索操作，不会修改，所以无论何时向下遍历就可以释放当前page的读锁
 *  3.2 插入操作，同上，先加写锁，然后将当前page加入事务PageSet，然后分情况决定要不要释放前面的一些锁(情况同上面：对于cNode 叶<MaxSize-1;非叶<MaxSize)
 *  3.3 删除操作，同上，加写锁，然后将当前page加入事务PageSet，当cNode>MinSize时，才可安全释放前面的锁
 * 4. 然后此时已经获得对应的叶节点的页，锁也已经是最局部的锁，返回
 *
 * 注意：释放锁的页面一定还不包含当前正在判断是否安全的页面，比如比较cNode,就算安全，cPage也还没加入PageSet中，就算修改cPage，也不影响PageSet中页
 *      也就是说，此时，transaction的PageSet中的page一定都是干净页，且这个事务不会再修改它们！！！
 *      这也就是为什么调用的释放锁的函数，Unpin时，isDirty = false 嘻嘻:)
 * */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost, bool rightMost) -> Page * {
  // 是查询事务：不能同时是最左/最右；不是查询事务(插入删除)：事务不能为空
    assert(operation == Operation::SEARCH ? !(leftMost && rightMost) : transaction != nullptr);
    assert(root_page_id_ != INVALID_PAGE_ID);
    auto page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    /*
     * 先解锁再上锁(解锁的原因是在调用FindLeaf的前，会对root_page_id上锁，这里解锁 重新对page上锁)，对于不同的事务，上的锁有锁不同
     * 上锁是对page上锁
     * */
    if(operation == Operation::SEARCH){
      // id使用完毕，现在在使用page,所以对page上读锁
      root_page_id_latch_.RUnlock();
      page->RLatch();
    } else{
      // 插入删除都要上写锁
      page->WLatch();
      // 1. 此时是根节点最小size不小于2(除非其只有一个节点).才能保证其作为扉页节点的属性，
      // 所以删除操作时，Size>2就可以释放这个事务前面的事务锁(其实也没有啥锁)
      if(operation == Operation::DELETE && node->GetSize() > 2){
        ReleaseLatchFromQueue(transaction);
      }
      if(operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1){
        // 是叶节点，插入一个也不会发生分裂,那么在这个之前的锁都可以释放啦
        ReleaseLatchFromQueue(transaction);
      }
      // 对于内部节点，要注意key < MaxSize - 1,但是内部节点的a个key包含(a+1)个KV对哦！而GetSize()指的是KV对的数量，所以内部节点多1
      if(operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()){
        ReleaseLatchFromQueue(transaction);
      }
    }

    while(!node->IsLeafPage()){
      auto *internalNode = reinterpret_cast<InternalPage *>(node);

      page_id_t childPageId; // 下一层的pageId
      if(leftMost){
        childPageId = internalNode->ValueAt(0);
      }else if(rightMost){
        childPageId = internalNode->ValueAt(internalNode->GetSize()-1);
      } else {
        childPageId = internalNode->Lookup(key,comparator_); // 找到应在的那个区间的value，即下一层的page id
      }
      assert(childPageId > 0);
      // 找到了下一层的id后，要切换到下一层并释放当前层page_id
      auto childPage = buffer_pool_manager_->FetchPage(childPageId);
      auto childNode = reinterpret_cast<BPlusTreePage *>(childPage->GetData());

      if(operation == Operation::SEARCH){
        // 查询肯定是不修改数据的，要跳转的页面上读锁，当前页面接触读锁，并接触当前页面在Buffer pool中的pin(不脏读)
        childPage->RLatch();
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      } else if(operation == Operation::INSERT){
        childPage->WLatch();
        transaction->AddIntoPageSet(page);
        // 同前面，当下一层就算插入也一定不会split时：
        // 是叶节点，且size < maxSize-1；
        // 内部节点，且size < maxSize
        if(childNode->IsLeafPage() && childNode->GetSize() < childNode->GetMaxSize() - 1){
          ReleaseLatchFromQueue(transaction);
        }
        if(!childNode->IsLeafPage() && childNode->GetSize() < childNode->GetMaxSize()){
          ReleaseLatchFromQueue(transaction);
        }
      } else if(operation == Operation::DELETE){
        // 删除
        childPage->WLatch();
        transaction->AddIntoPageSet(page);
        // 大于最小KV数，就算删除也不会合并
        if(childNode->GetSize() > childNode->GetMinSize()){
          ReleaseLatchFromQueue(transaction);
        }
      }
      page = childPage;
      node = childNode;
    }
    return page;
}


/*
 * 用于释放事务中当前存放的安全页的锁，方便其他事务操作，保证操作的局部性，避免死锁，提升并发性能
 * FindLeaf函数中已经提到，此时事务PageSet中的页一定都是当前为被修改且也不会被这个事务修改的页，所以可以大胆释放并UnPin
 * B+树中释放事务中积累的页面锁，并处理与这些页面相关的后续操作Unpin
 * 对于Insert Remove操作：
 * 在一开始时，会对root_page_id_latch上写锁，并插入nullptr;
 * 后面遍历到的页面则会上page->WLatch 页面写锁，并插入页面指针。
 * 所以在逐步释放时，循环遍历事务中的页面集合：
 * 对于nullptr，root_page_id_latch_.WUnlock();
 * 对于page，释放写锁，并释放Buffer pool中对该页面的pin
 * */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
    while (!transaction->GetPageSet()->empty()){
      Page *page = transaction->GetPageSet()->front();
      transaction->GetPageSet()->pop_front();
      if(page == nullptr){
        this->root_page_id_latch_.WUnlock();
      } else {
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
    }
}


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * Search操作：
 * 1. 先上读锁
 * 2. 调用FindLeaf函数，查找key对应叶值可能在的页
 * 3. 在可能的Leaf页中查找key对应的value
 * 4. 查完后就可以解除读锁，并将Buffer pool中的页解除Pin(好像都是先解锁，在UNPin捏？)
 * 5. 判断key是否存在，存在将值压入result，反之略(存在多次调用查找估计害)
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
    root_page_id_latch_.RLock();
    auto leafPage = FindLeaf(key,Operation::SEARCH,transaction);
    auto *node = reinterpret_cast<LeafPage *>(leafPage->GetData());

    ValueType v;
    auto existed = node->Lookup(key,&v,comparator_);

    leafPage->RUnlatch();
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
    if(!existed){
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
    root_page_id_latch_.WLock();
    transaction->AddIntoPageSet(nullptr); // nullptr就是root_page_id_latch_
    if(IsEmpty()){
      // 当前为空，新建B+树，插入k,v
      // 然后释放相应的锁，就可以结束辣 比心！
      StartNewTree(key,value);
      ReleaseLatchFromQueue(transaction);
      return true;
    }
    // 非空，在叶节点页插入
    return InsertIntoLeaf(key,value,transaction);
}

/*
 * 开始创建一个B+树，并将<k,v>插入
 * 1. 先从buffer pool中NewPage，若new失败，证明无空闲，报错
 * 2. new成功，将这个新page转为LeafNode，Init对应参数，并插入<k,v>
 * 3. 最后从Buffer pool中Unpin该page，别忘记isDirty为true
 * */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(page == nullptr){
      // Buffer pool中无法new新的page，报错
      throw Exception(ExceptionType::OUT_OF_MEMORY,"Cannot allocate new page");
  }

  auto leafNode = reinterpret_cast<LeafPage *>(page->GetData());
  leafNode->Init(root_page_id_,INVALID_PAGE_ID,leaf_max_size_);

  leafNode->Insert(key,value,comparator_);

  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}


/*
 * 1. 先调用FindLeaf函数，找到要插入的Page，并通过page转为对应的LeafNode
 * 2. 先插入，再根据插入后Size与MaxSize的关系判断是否需要split
 * 3. 三种情况
 *  3.1 重复插入，返回false
 *  3.2 未超员，直接插入
 *  3.3 满了，需要split，烦！并且一定是叶节点哇，所以要设置正确的叶磁盘页指针！
 *    3.3.1 下层分裂，并且分裂后要将新分裂的页的第一个key提到上层中插入
 *    3.3.2 然后释放当前锁，释放前面的锁，UnPin结束！
 * */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1. 先调用FindLeaf函数，找到要插入的Page，并通过page转为对应的LeafNode
  auto leafPage = FindLeaf(key,Operation::INSERT,transaction);
  auto *node = reinterpret_cast<LeafPage *>(leafPage->GetData());

  // 2. 先插入，再根据插入后Size与MaxSize的关系判断是否需要split
  // 其中需要注意的是(重复插入捏~)，如果原本有这个key，插入前后Size不变，此时可以直接返回
  auto size = node->GetSize();
  auto newSize = node->Insert(key,value,comparator_);

  // 3.1 重复插入，返回false
  if(newSize == size){
      // 重复插入
      ReleaseLatchFromQueue(transaction);
      leafPage->WUnlatch(); // ！！！始终要注意，ReleaseLatchFromQueue最后是不会加入最终查到的Leaf页的(因为不会加入PageSet)
      buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false); // 手动解锁后，立马跟着的是UnPin！！
      return false;
  }
  // 3.2 未超员，直接插入
  if(newSize < leaf_max_size_){
      // 未到最大Size，直接插入即可，不需要split
      ReleaseLatchFromQueue(transaction);
      leafPage->WUnlatch();
      buffer_pool_manager_->UnpinPage(leafPage->GetPageId(),true);
      return true;
  }

  // 3.3 满了，需要split，烦！并且一定是叶节点哇，所以要设置正确的叶磁盘页指针！
  auto splitNewLeafNode = Split(node);
  splitNewLeafNode->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(splitNewLeafNode->GetPageId());

  // 同时下层分裂后，要将新分裂的页的第一个key提到上层中插入
  auto risenKey = splitNewLeafNode->KeyAt(0);
  InsertIntoParent(node,risenKey,splitNewLeafNode,transaction);

  // 然后释放当前锁，释放前面的锁，UnPin结束！
  leafPage->WUnlatch();
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(splitNewLeafNode->GetPageId(), true);
  return true;
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
  page_id_t pageId;
  auto newPage = buffer_pool_manager_->NewPage(&pageId);

  if(newPage == nullptr){
      // 无法new新页了
      throw Exception(ExceptionType::OUT_OF_MEMORY,"Cannot allocate new newPage");
  }

  N *newNode = reinterpret_cast<N *>(newPage->GetData());
  newNode->SetPageType(node->GetPageType());

  if(node->IsLeafPage()){
      auto *leafNode = reinterpret_cast<LeafPage *>(node);
      auto *newLeafNode = reinterpret_cast<LeafPage *>(newNode);

      // 在初始化新子页时，表明新子Page的ParentPageId和旧子页的一样
      newLeafNode->Init(newPage->GetPageId(),leafNode->GetParentPageId(),leaf_max_size_);
      leafNode->MoveHalfTo(newLeafNode);
      // 更新叶磁盘页间链表指针的操作在调用split函数外的函数里有，严谨嘿！
  } else {
      auto *internalNode = reinterpret_cast<InternalPage *>(node);
      auto *newInternalNode = reinterpret_cast<InternalPage *>(newNode);

      // 在初始化新子页时，表明新子Page的ParentPageId和旧子页的一样
      newInternalNode->Init(newPage->GetPageId(),internalNode->GetParentPageId(),internal_max_size_);
      internalNode->MoveHalfTo(newInternalNode,buffer_pool_manager_);
  }
  return newNode;
}

/*
 * 将new_node的page_id作为V,把<key,V>插入上层父节点页中
 * 三种情况：
 * 1. 当上层父节点插入也不超员，最简单！直接插入，然后Unpin就好啦
 * 2. 当上层无父节点(根节点分裂唉)，要NewPage作为新的根页，并更新Header中存放的根页id，同时更新两个node的ParentPageId
 * 3. 最麻烦ORZ，上层有父，且插入后父需要分裂：
 *  * 1. 首先，分配一定的内存mem来临时存储父节点的内容和额外的一个键值对；并将父节点当前完整内容复制到新分配的内存中
 *  * 2. 插入新的键值对(正好占满mem)，然后在mem转化的node上调用split分裂，同时返回一个存放剩余键值对的新父页
 *  * 3. 父页分裂所以也要把分裂后的KV上提给父父页，需要存一下
 *  * 4. 同时将分裂后的应属于原父节点的内容再写回原父节点
 *  * 5. 然后递归调用当前函数，将父页分裂后的新KV对存入父父页
 *  * 6. 最后解除固定，清理内存(对于锁的释放早在递归调用中就已经完成啦~)
 * */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, Transaction *transaction) {
  if(old_node->IsRootPage()){
      // 根节点分裂，需要创建新页为根节点页
      auto rootPage = buffer_pool_manager_->NewPage(&root_page_id_);

      if(rootPage == nullptr){
        throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
      }
      auto *newRootNode = reinterpret_cast<InternalPage *>(rootPage->GetData());
      newRootNode->Init(root_page_id_,INVALID_PAGE_ID,internal_max_size_);

      newRootNode->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());

      old_node->SetParentPageId(newRootNode->GetPageId());
      new_node->SetParentPageId(newRootNode->GetPageId());

      buffer_pool_manager_->UnpinPage(rootPage->GetPageId(),true);

      UpdateRootPageId(0); // 这个函数的意思是更新(0)/插入(1)根节点页的ID,因为此时已经有根节点页ID了，只需要更新，不需要插入

      ReleaseLatchFromQueue(transaction);
      return ;
  }
  // 非根节点分裂,需要将new_node的第一个key(在这里即传入的参数key)提给父节点(要注意判断父节点页中Size数量，需要split咩)
  // 获取父节点页与Node
  auto parentPage = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parentNode = reinterpret_cast<InternalPage *>(parentPage->GetData());

  if(parentNode->GetSize() < internal_max_size_){
      // 还可以至少再插入一个而不split，直接插入
      // 插入后就安全了，逐级释放锁，然后UnPin
      parentNode->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
      ReleaseLatchFromQueue(transaction);
      buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
      return ;
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
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType)*(parentNode->GetSize()+1)];
  auto *copyOldParentNode = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem,parentPage->GetData(),INTERNAL_PAGE_HEADER_SIZE+sizeof(MappingType) * parentNode->GetSize());
  // 2.
  copyOldParentNode->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
  auto newSplitParentNode = Split(copyOldParentNode);
  // 3.
  KeyType newParentKey = newSplitParentNode->KeyAt(0);
  // 4.
  std::memcpy(parentPage->GetData(),mem,INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copyOldParentNode->GetMinSize());
  // 5.
  InsertIntoParent(parentNode,newParentKey,newSplitParentNode,transaction);
  // 6.
  buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(newSplitParentNode->GetPageId(),true);
  delete[] mem;
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
 *
 * 删除时先对根页上写锁
 * 如果是空的，直接释放安全锁，返回
 * 非空，FindLeaf寻找<k,v>所在leafPage:
 *  在leafPage中未找到要删除的kv对，释放锁，UnPin结束
 *  找到就删除，然后根据删除后的Size看看需不需要合并(CoalesceOrRedistribute函数)
 *  好了此时这个节点以及删除后的其他节点的调整已经结束，需要的就是释放当前这个页的锁，并UnPin
 *  同时循环清除那些放在transaction的deletedPageSet中的页(这些页可能是已经空了，也可能是合并过程中剩下来的不要的页，在最后统一清空一下！)
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if(IsEmpty()){
      // 删除一个空B+树中的内容，释放锁返回
      ReleaseLatchFromQueue(transaction);
      return ;
  }
  // 找到应该删除的KV对所在的叶节点Page
  auto leafPage = FindLeaf(key,Operation::DELETE,transaction);
  auto leafNode = reinterpret_cast<LeafPage *>(leafPage->GetData());

  if(leafNode->GetSize() == leafNode->RemoveAndDeleteRecord(key,comparator_)){
      // 在leafPage中未找到要删除的kv对，释放锁，UnPin结束
      ReleaseLatchFromQueue(transaction);
      leafPage->WUnlatch();
      buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
      return;
  }
  // 此时已经删除了，要根据删除后的Size看看需不需要合并
  auto nodeShouldDelete = CoalesceOrRedistribute(leafNode,transaction);
  // 好了此时这个节点以及删除后的其他节点的调整已经结束，需要的就是释放当前这个页的锁，并UnPin
  // 同时循环清除那些放在transaction的deletedPageSet中的页(这些页可能是已经空了，也可能是合并过程中剩下来的不要的页，在最后统一清空一下！)
  leafPage->WUnlatch();

  if(nodeShouldDelete) {
      transaction->AddIntoDeletedPageSet(leafNode->GetPageId());
  }

  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(),true);

  // 循环清除需要delete的Page!
  std::for_each(transaction->GetDeletedPageSet()->begin(),transaction->GetDeletedPageSet()->end(),
                [&bmp = buffer_pool_manager_](const page_id_t pageId) {
                  bmp->DeletePage(pageId);
                });
  transaction->GetDeletedPageSet()->clear();
}

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

  if(node->GetSize() >= node->GetMinSize()){
    // 1. 删除后数量没低于最小值，不需要合并，好耶！
      ReleaseLatchFromQueue(transaction);
      return false;
  }

  if(node->IsRootPage()){
      // 是根节点的删除
    auto rootShouldDel = AdjustRoot(node);
    // 调整完毕，可以直接返回了
    ReleaseLatchFromQueue(transaction);
    return rootShouldDel;
  }

  // 非根节点的删除，且节点删除后需要进行合并或者更改结构
  auto parentPage = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parentNode = reinterpret_cast<InternalPage *>(parentPage->GetData());
  // 先获得当前删除节点的叶在父节点中的index
  auto cIndex = parentNode->ValueIndex(node->GetPageId());

  if(cIndex > 0){
    // 1. 先看删除节点页挨着的左边页是否可以贡献一个
    auto leftCPage = buffer_pool_manager_->FetchPage(parentNode->ValueAt(cIndex-1));
    leftCPage->WLatch();
    N *leftCNode = reinterpret_cast<N *>(leftCPage->GetData());

    if(leftCNode->GetSize() > leftCNode->GetMinSize()){
        // 1.1 挨着的左边页可以直接匀过来一个KV对！！么么
        // 1.2 然后释放前面的锁
        // 1.3 将父节点页UnPin(锁已释放)
        // 1.4 将当前节点锁先释放，然后UnPin 匀给自己KV对的页(自己在Remove页面最后释放锁+UnPin捏)
        Redistribute(leftCNode,node,parentNode,cIndex,true);
        ReleaseLatchFromQueue(transaction);
        buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
        leftCPage->WUnlatch();
        buffer_pool_manager_->UnpinPage(leftCNode->GetPageId(),true);
        return false;
    }

    // 左边节点页不具备借我一个的条件，啊啊啊，只能合并了，你瞅瞅图啥 凋谢orz
    // 合并相邻+自己，同时将父节点页上面我们俩的key合成一个，其实就是删除被合并进去的那个key就好啦~
    // Coalesce在合并完子节点页，执行完父节点页相应KV删除后，会递归调用当前函数，直至所有删除结束！
    auto parentNodeShouldDel = Coalesce(leftCNode,node,parentNode,cIndex,transaction);

    if(parentNodeShouldDel) {
        transaction->AddIntoDeletedPageSet(parentNode->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
    leftCPage->WUnlatch();
    buffer_pool_manager_->UnpinPage(leftCNode->GetPageId(),true);
    return true;
  }

  // 感觉下面应该是 == 0，也就是在最左边
  if(cIndex != parentNode->GetSize() - 1){
    // 当前在最左边，只能选择右边相邻节点页借，其余同理
    auto rightCPage = buffer_pool_manager_->FetchPage(parentNode->ValueAt(cIndex+1));
    rightCPage->WLatch();
    N *rightCNode = reinterpret_cast<N *>(rightCPage->GetData());

    if(rightCNode->GetSize() > rightCNode->GetMinSize()){
        // 同上，可以借，直接借
        Redistribute(rightCNode,node,parentNode,cIndex, false);

        ReleaseLatchFromQueue(transaction);
        buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
        rightCPage->WUnlatch();
        buffer_pool_manager_->UnpinPage(rightCNode->GetPageId(),true);
        return false;
    }

    // 同上需要合并
    // 父节点页中要删除的key要换一下，是右边页的page_id所在的inde的key
    // 调用合并函数时小心，因为第一个为左边页，第二个为右边页
    auto needRemoveIndex = parentNode->ValueIndex(rightCNode->GetPageId());
    auto parentNodeShouldDel = Coalesce(node,rightCNode,parentNode,needRemoveIndex,transaction);
    transaction->AddIntoDeletedPageSet(rightCNode->GetPageId());
    if(parentNodeShouldDel) {
        transaction->AddIntoDeletedPageSet(parentNode->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
    rightCPage->WUnlatch();
    buffer_pool_manager_->UnpinPage(rightCNode->GetPageId(),true);
    return true;
  }
  return false;
}

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
  if(!node->IsLeafPage() && node->GetSize() == 1){
    // 如果当前节点页是内部节点页，且只有一个KV对，则整个树可以在不损失任何信息的情况下，将这个节点页与下面的合并
    auto *rootNode = reinterpret_cast<InternalPage *>(node);
    auto onlyChildPage = buffer_pool_manager_->FetchPage(rootNode->ValueAt(0));
    auto *onlyChildNode = reinterpret_cast<BPlusTreePage *>(onlyChildPage->GetData());
    onlyChildNode->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = onlyChildNode->GetPageId();

    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(onlyChildPage->GetPageId(),true);
    return true;
  }
  if(node->IsLeafPage() && node->GetSize() == 0){
    // 根节点是叶节点，且无元素了，代表删除了B+树中的最后一个元素
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  // 不需要调整树root的结构
  return false;
}

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
    // 如果是叶节点页的话，可以直接借
    auto *leafNode = reinterpret_cast<LeafPage *>(node);
    auto *neighborLeafNode = reinterpret_cast<LeafPage *>(neighbor_node);

    if(!from_prev){
        // 向右边的节点页借1：实际上是将右边节点页的第一个移到当前节点页的最后
        // 由于父节点中对应位置的key是各个子节点页的第一个key(叶节点页，有值)，value是各个子节点页的page_id
        // 所以还要更新右边节点页在父节点页中对应位置的key值
        neighborLeafNode->MoveFirstToEndOf(leafNode);
        parent->SetKeyAt(index+1,neighborLeafNode->KeyAt(0));
    } else {
        // 同理，向左边的节点页借1：实际上是将左边节点页的最后一个移到当前节点页的开头
        // 其余逻辑同上
        neighborLeafNode->MoveLastToFrontOf(leafNode);
        parent->SetKeyAt(index, leafNode->KeyAt(0));
    }
  } else {
    // 是内部节点页之间的借
    auto *internalNode = reinterpret_cast<InternalPage *>(node);
    auto *neighborInternalNode = reinterpret_cast<InternalPage *>(neighbor_node);

    if(!from_prev){
        // 向右边的节点页借1：实际上是将右边节点页的第一个移到当前节点页的最后
        // 与叶节点页不同的是，内部节点页的第一个key为null，实际保存在上层的parent中(也就是上层自己对应位置的key值)，所以要作为参数传入
        // 其余与上同
        neighborInternalNode->MoveFirstToEndOf(internalNode, parent->KeyAt(index+1),buffer_pool_manager_);
        // 这里有个小trick，原本新移动后的neighborInternalNode的第一个key也是空，但这里没有人为设置为空，只是会避免使用，所以可能更好的方式是在移动前，存放一下Key2 P
        parent->SetKeyAt(index+1,neighborInternalNode->KeyAt(0));
    } else {
        // 同理
        neighborInternalNode->MoveLastToFrontOf(internalNode, parent->KeyAt(index),buffer_pool_manager_);
        parent->SetKeyAt(index,internalNode->KeyAt(0));
    }
  }
}


/*
 * 用于明确要合并两个子节点页，返回最后是否对树的结构进行了改变重建，即层数发生了改变这种
 * 合并：
 * 1. 将node中的KV对加入到neighbor_node的kV对后
 * 2. 同时删除在Parent中的index位置的key,v对(因为这个指针指向的叶被合并起来啦，还要加入transaction的deletePageSet，最后统一删除捏)
 * 3. 因为父节点也删除节点了，所以要将父节点作为node，重新递归调用一遍CoalesceOrRedistribute判断哦！
 * */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index, Transaction *transaction) -> bool {
  auto middle_key = parent->KeyAt(index); // 父节点页中因为合并需要删除的key

  if(node->IsLeafPage()){
    auto *leafNode = reinterpret_cast<LeafPage *>(node);
    auto *prevLeafNode = reinterpret_cast<LeafPage *>(neighbor_node);
    leafNode->MoveAllTo(prevLeafNode);
  } else {
    auto *internalNode = reinterpret_cast<InternalPage *>(node);
    auto *prevInternalNode = reinterpret_cast<InternalPage *>(neighbor_node);
    internalNode->MoveAllTo(prevInternalNode,middle_key,buffer_pool_manager_);
  }

  parent->Remove(index); // 删除合并在里面的那个key
  return CoalesceOrRedistribute(parent,transaction); // 因为父节点也删除节点了，所以要将父节点作为node，重新递归调用一遍判断哦！
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leftmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leaf_page = FindLeaf(key, Operation::SEARCH);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto rightmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, false, true);
  auto *leaf_node = reinterpret_cast<LeafPage *>(rightmost_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_page, leaf_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  root_page_id_latch_.RLock();
  root_page_id_latch_.RUnlock();
  return root_page_id_;
}

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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

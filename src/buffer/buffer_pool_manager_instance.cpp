//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer
  // 缓冲池中最多可以放下多少page，由于frame_id是固定连续的一部分数，所以可以直接用page_[frame_id]来查找对应的Page
  pages_ = new Page[pool_size_];
  // hash用来完成从page_id到frame_id的映射。
  // hash会将page_id映射成连续的数字存放为key，对应的value是frame_id
  // 这样的话，通过hash，可以用page_id查找对应的frame_id。
  // 同时page_id与frame_id又不是完全绑定死的(由LRU-k我们知道，frame_id是固定的一些数),
  // 只是在你插入的时候，给予一个绑定，淘汰后绑定又消失
  // 存放着Buffer pool中的现有的页的<p_id,f_id>
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  // LRU中最大存放的页为pool_size，其跟踪每个缓存page，用于判断该page应当放入哪里，什么时候移除
  // 这里的key又变成了frame_id
  replacer_ = new LRUKReplacer(pool_size, replacer_k);
  // 多的其实是移除后是否是脏页，脏页需要重新写入磁盘

  // 空白页(干净)，未放缓存页
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  /*
   * 在Buffer pool中创建插入一个新页，新页id存入*page_id中，新页暂无数据
   * 1. 遍历pages_(0~pool_size-1)
   * 2. 如果buffer pool(page_)内的所有页都正在被使用且不可被淘汰(pinCount > 0)，不可插入，nullptr
   * 3. 表示存在未使用的干净页或虽然pool全满但存在可被淘汰的页
   *  3.1 存在干净页 -> free_list_非空，选出一个空闲的frame_id 进行步骤3
   *  3.2 全满但存在可被淘汰的 -> 利用LRU-k淘汰一个最优先被淘汰的(注意如果是脏页要写回磁盘)，清空该页，返回其frame_id 进行步骤3
   * 4. 当前的frame_id代表*page_id可写入的，按步骤写入：
   *   Page更新入pages_
   *   插入进page_table_(记录下<page_id,frame_id>键值对)
   *   replacer_(LRU-k)要更新访问信息等
   * */
  std::scoped_lock<std::mutex> lock(latch_);
  bool hasPageCanFree = false;
  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].GetPinCount() == 0){
      // 代表可以被淘汰或者为空
      hasPageCanFree = true;
      break ;
    }
  }
  if(!hasPageCanFree){
    // 2.全部不可淘汰，全满
    return nullptr;
  }
  // 3.
  *page_id = AllocatePage(); // 代表可以放页，所以先初始化页
  frame_id_t frameId;
  if(!free_list_.empty()){
    // 3.1
    frameId = free_list_.front();
    free_list_.pop_front();
  } else {
    // 3.2
    replacer_->Evict(&frameId);
    page_id_t oldPageId = pages_[frameId].GetPageId();
    if(pages_[frameId].IsDirty()){
      // 是脏数据，需要先写回磁盘
      disk_manager_->WritePage(oldPageId,pages_[frameId].GetData());
      pages_[frameId].is_dirty_ = false;
    }
    // 把当前要写入新数据的页先清空成干净页
    pages_[frameId].ResetMemory();
    // 移除page_table_中原有的<page_id,frame_id>键值对(下面要插入成新的)
    page_table_->Remove(oldPageId);
  }
  // 4
  pages_[frameId].page_id_ = *page_id;
  pages_[frameId].pin_count_ = 1;

  page_table_->Insert(*page_id,frameId);

  // 这里其实就是插入
  replacer_->RecordAccess(frameId);
  replacer_->SetEvictable(frameId, false);

  return &pages_[frameId];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  /*
   * NewPgImp 是插入一个新页，FetchPgImp是获取id为page_id的页，如果没在buffer pool中，就先插入，再返回
   * 1. 先查看这个页是否已经在buffer pool中了(因为是通过page_id查找，所以只能在page_table_查找<k,v>)
   *    这个页已经在buffer pool中了 -> 直接返回
   * 2. 不在时，查看是否存在干净页，存在就插入 -> 返回
   * 3. 遍历buffer pool中所有页，查看是否可淘汰
   *  3.1 若全不可淘汰，且这个要插入的页未在buffer pool中(即需要从磁盘中获取且插入)，返回nullptr
   *  3.2 存在可被淘汰的，插入方式同NewPgImp的3、4
   * */
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frameId;
  if(page_table_->Find(page_id,frameId)){
    // 1. 找到了; pin_count++, LRU访问+set不可淘汰
    ++pages_[frameId].pin_count_;
    replacer_->RecordAccess(frameId);
    replacer_->SetEvictable(frameId, false);
    return &pages_[frameId];
  }
  // 没有只能从磁盘中读然后插入了
  // 能不能插入
  bool hasPageCanFree = false;
  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].GetPinCount() == 0){
      hasPageCanFree = true;
      break ;
    }
  }

  if(!hasPageCanFree){
    // 3.1
    return nullptr;
  }

  if(!free_list_.empty()){
    // 2. 存在干净页
    frameId = free_list_.front();
    free_list_.pop_front();
  } else{
    // 3.2 需要先淘汰，有脏页还要写回
    replacer_->Evict(&frameId);
    page_id_t oldPageId = pages_[frameId].GetPageId();
    page_table_->Remove(oldPageId);

    if(pages_[frameId].IsDirty()){
      disk_manager_->WritePage(oldPageId,pages_[frameId].GetData());
      pages_[frameId].is_dirty_ = false;
    }

    pages_[frameId].ResetMemory();
    page_table_->Remove(oldPageId);
  }

  page_table_->Insert(page_id,frameId); // 插入新的键值对

  pages_[frameId].page_id_ = page_id;
  pages_[frameId].pin_count_ = 1;
  // 将磁盘中page_id的数据读入
  disk_manager_->ReadPage(page_id,pages_[frameId].GetData()); // 相较于new不同的在于，插入后，将页中数据从磁盘读入！

  replacer_->RecordAccess(frameId);
  replacer_->SetEvictable(frameId, false);

  return &pages_[frameId];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  /*
   * 这个应该是访问完页之后调用的函数：从缓冲池中解除目标页的锁定，如果is_dirty为true代表修改了，需要设置为脏页
   * 1. 先检查缓冲池中有没有该页，没有或者其引用计数已变为0(当前页已被释放)，说明有问题 -> 返回false、
   * 2. 缓冲池中有且引用计数不为0，此时我使用完了，引用计数--，并设置is_dirty
   * 3. 如果该页引用计数变为0，代表可以被淘汰，在replacer_中设置上为true
   * */
  std::scoped_lock<std::mutex> lock(latch_);
  // 由于是通过page_id查找，需要通过page_table映射
  frame_id_t frameId;
  // 1.
  if(!page_table_->Find(page_id,frameId)){
    return false;
  }
  // 1.
  if(pages_[frameId].GetPinCount() == 0){
    return false;
  }
  // 3. 存在该页，引用计数--，更新is_dirty(注意只能更新为脏页，原为脏页，但此次没更新，还是脏页)
  --pages_[frameId].pin_count_;
  if(is_dirty){
    // 脏可以更新为脏，脏不可更新为不脏
    pages_[frameId].is_dirty_ = is_dirty;
  }
  if(pages_[frameId].GetPinCount() == 0){
    // 减为0代表没人正在使用，可以被淘汰
    replacer_->SetEvictable(frameId, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  /*
   * 直接将指定页刷新回磁盘
   * 1. 如果id是INVALID_PAGE_ID，返回false
   * 2. 如果在缓冲区找不到该page_id，返回false
   * 3. 找到后，将内容更新回磁盘，然后is_dirty设置为false(不用移除这个page)
   * */
  std::scoped_lock<std::mutex> lock(latch_);
  // 1.
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  // 2.
  frame_id_t frameId;
  if(!page_table_->Find(page_id,frameId)){
    return false;
  }
  // 3.
  disk_manager_->WritePage(page_id,pages_[frameId].GetData());
  pages_[frameId].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for(size_t i = 0; i < pool_size_; ++i){
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  /*
   * 删除这个页(这个页需要能被删除)，是脏页也不许要写回，直接释放空间
   * 1. 如果这个页不在buffer pool中，或者对应的引用计数>0(不可被淘汰)，返回false
   * 2. 这个页在，且引用计数为0 -> 可以被删除
   *  从page_table_、pages_、replacer中全面删除释放空间
   *  并将清空后的frame_id放回free_list_ -> 该frame_id对应的是干净页，可以被分配...
   * */
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frameId;
  // 1.
  if(!page_table_->Find(page_id,frameId)){
    return false;
  }
  // 1.
  if(pages_[frameId].GetPinCount() > 0){
    return false;
  }
  // 2. 存在，要删除
  replacer_->Remove(frameId);

  pages_[frameId].ResetMemory();
  pages_[frameId].page_id_ = INVALID_PAGE_ID;
  pages_[frameId].pin_count_ = 0;
  pages_[frameId].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frameId);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

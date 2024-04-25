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
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
// NewPgImp
// 在buffer pool中插入一个新页, page_id为返回的id
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1. 优先找可用的page, 遍历所有pages来找unpinned的page
  bool has_free_page = false;
  for(size_t i = 0; i < this->pool_size_; i++) {
    if(this->pages_[i].GetPinCount() == 0) {
      has_free_page = true;
      break;
    }
  }
  // 2. 如果找不到unpinned的page, return nullptr;
  if(has_free_page == false) {
    return nullptr;
  }
  // 3. 当前存在unpinned的页，分两种处理
  *page_id = AllocatePage();
  frame_id_t frame_id;
  if(!this->free_list_.empty()) {
    // 3.1 存在干净的页, 从free_list中选择一个frame即可;
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  }else {
    // 3.2 存在可以淘汰的页, 利用LRU-K, 淘汰一个frame使用;
    this->replacer_->Evict(&frame_id);
    auto old_page_id = this->pages_[frame_id].GetPageId();
    auto old_page_data = this->pages_[frame_id].GetData();
    if(this->pages_[frame_id].IsDirty()) {
      // 替换前如果是Dirty数据要先写回磁盘
      this->disk_manager_->WritePage(old_page_id, old_page_data);
      this->pages_[frame_id].is_dirty_ = false;
    }
    // 清理旧frame的page信息
    this->pages_[frame_id].ResetMemory();
    this->page_table_->Remove(old_page_id);
  }
  // 4. 将page写入对应的frame
  // 4.1 将page_id放入pages
  this->pages_[frame_id].page_id_ = *page_id;
  this->pages_[frame_id].pin_count_ = 1;
  // 4.2 更新page_tables
  this->page_table_->Insert(*page_id, frame_id);
  // 4.3 更新LRU
  this->replacer_->RecordAccess(frame_id);
  this->replacer_->SetEvictable(frame_id, false);
  // 5. return page
  return &this->pages_[frame_id];
}
// FetchPgImp
// 获取page_id的页, 与NewPgImp整体流程相似;
// 区别仅仅在于, New是直接插入, Fetch是获取此页, 如果没有则插入
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 1. 先检查bufferpool中是否已经缓存了此page
  frame_id_t frame_id;
  if(this->page_table_->Find(page_id, frame_id)) {
    // 如果缓存了, 增加pin, LRU访问, 然后返回即可
    ++this->pages_[frame_id].pin_count_;
    this->replacer_->RecordAccess(frame_id);
    this->replacer_->SetEvictable(frame_id, false);
    return &this->pages_[frame_id];
  }
  // 2. 此页不存在, 需要从磁盘中读取并插入到buffer pool, 与New最大区别在于要从磁盘读page的数据
  //========================重复NewPgImp的部分=======================//
  // 1. 优先找可用的page, 遍历所有pages来找unpinned的page
  bool has_free_page = false;
  for (size_t i = 0; i < this->pool_size_; i++) {
    if (this->pages_[i].GetPinCount() == 0) {
      has_free_page = true;
      break;
    }
  }
  // 2. 如果找不到unpinned的page, return nullptr;
  if (has_free_page == false) {
    return nullptr;
  }
  // 3. 当前存在unpinned的页，分两种处理
  if (!this->free_list_.empty()) {
    // 3.1 存在干净的页, 从free_list中选择一个frame即可;
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else {
    // 3.2 存在可以淘汰的页, 利用LRU-K, 淘汰一个frame使用;
    this->replacer_->Evict(&frame_id);
    auto old_page_id = this->pages_[frame_id].GetPageId();
    auto old_page_data = this->pages_[frame_id].GetData();
    if (this->pages_[frame_id].IsDirty()) {
      // 替换前如果是Dirty数据要先写回磁盘
      this->disk_manager_->WritePage(old_page_id, old_page_data);
      this->pages_[frame_id].is_dirty_ = false;
    }
    // 清理旧frame的page信息
    this->pages_[frame_id].ResetMemory();
    this->page_table_->Remove(old_page_id);
  }
  // 4. 将page写入对应的frame
  // 4.1 将page_id放入pages
  this->pages_[frame_id].page_id_ = page_id;
  this->pages_[frame_id].pin_count_ = 1;
  // 4.2 更新page_tables
  this->page_table_->Insert(page_id, frame_id);
  // NOTE: 将磁盘中page_id对应的数据读入, 这是与New不同的点
  this->disk_manager_->ReadPage(page_id, this->pages_[frame_id].GetData());
  // 4.3 更新LRU
  this->replacer_->RecordAccess(frame_id);
  this->replacer_->SetEvictable(frame_id, false);
  // 5. return page
  return &this->pages_[frame_id];
}
// UnpinPgImp
// 将buffer pool中的page_id unpinned, 解决目标页的锁定; dirty标识该页是否被修改
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 1. 先检查buffer pool中是否有page_id, 没有或pin=0说明已被释放
  frame_id_t frame_id;
  if(!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if(this->pages_[frame_id].GetPinCount() == 0) {
    return false;
  }
  // 2. 页存在, 则减少pin
  --this->pages_[frame_id].pin_count_;
  // 3. 更新dirty状态, 必须是dirty才可以去修改page的状态
  if(is_dirty) {
    this->pages_[frame_id].is_dirty_ = is_dirty;
  }
  // 4. 如果pin被减为0, 说明page可以被释放;
  if(this->pages_[frame_id].GetPinCount() == 0) {
    this->replacer_->SetEvictable(frame_id, true);
  }
  return true;
}
// FlushPgImp
// 将指定页page_id刷回磁盘
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 1. 如果是INVALID_PAGE_ID, return false;
  if(page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 2. 如果buffer pool中找不到此page_id, return false
  frame_id_t frame_id;
  if(!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // 3. 找得到则将page更新会磁盘, dirty=false
  this->disk_manager_->WritePage(page_id, this->pages_[frame_id].GetData());
  this->pages_[frame_id].is_dirty_ = false;
  return true;
}
// FlushPgImp
// 将所有page刷回磁盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(this->latch_);
  for(size_t i = 0; i < this->pool_size_; i++) {
    this->FlushPgImp(this->pages_[i].GetPageId());
  }
}
// DeletePgImp
// 从bufferpool中删除一个page
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 1. 如果page_id不在buffer pool中, return true;
  frame_id_t frame_id;
  if(!this->page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // 2. 如果page_id处于pinned状态, return false;
  if(this->pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  // 3. 从buffer pool中删除此page
  // 3.1 从LRU中将对应frame删除
  this->replacer_->Remove(frame_id);
  // 3.2 将frame加入free_list
  this->free_list_.push_back(frame_id);
  // 3.3 reset page的内存和元数据
  this->pages_[frame_id].ResetMemory();
  this->pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  this->pages_[frame_id].pin_count_ = 0;
  this->pages_[frame_id].is_dirty_ = false;
  // 3.4 调用DeallocatePage来释放磁盘中的page
  this->DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

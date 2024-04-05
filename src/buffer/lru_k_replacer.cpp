//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 删除一个最应删除的，将删除的id存入frame_id中并返回
  // 先看history_list_，没有则再看cache_list_
  if(curr_size_ == 0){ // 空
    return false;
  }
  // 从最老的开始遍历history_list_
  for(auto item = history_list_.rbegin(); item != history_list_.rend(); ++item){
    auto item1 = *item;
    if(is_evictable_[item1]){
      // 找到了一个最符合淘汰条件的
      // list要用指向的迭代器删除，map可以直接用key指向位置并删除
      history_list_.erase(history_map_[item1]);
      history_map_.erase(item1);

      access_count_[item1] = 0;
      is_evictable_[item1] = false;
      --curr_size_;
      *frame_id = item1;
      return true;
    }
  }

  for(auto item = cache_list_.rbegin(); item != cache_list_.rend(); ++item){
    auto item1 = *item;
    if(is_evictable_[item1]){
      cache_list_.erase(cache_map_[item1]);
      cache_map_.erase(item1);

      access_count_[item1] = 0;
      is_evictable_[item1] = false;  // frame_id是page_id映射过来的表，理论上应该所有可能的frame_id都在里面
      --curr_size_;
      *frame_id = item1;
      return true;
    }
  }
  // 没有可以删的地方
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if(frame_id > static_cast<int>(replacer_size_)){
    // 超出了规定的最大frame_id映射，抛出异常
    throw std::exception();
  }
  // 现在访问frame_id数据
  ++access_count_[frame_id];
  if(access_count_[frame_id] == k_){
    // 要加入cache_list_与map中，并从history中删除(如果有的话)
    cache_list_.push_front(frame_id); // 插入到list最前面，begin位置
    cache_map_[frame_id] = cache_list_.begin();

    if(history_map_.count(frame_id)){
      // 存在，从history中删除，避免k=1的情况
      history_list_.erase(history_map_[frame_id]);
      history_map_.erase(frame_id);
    }
  } else if(access_count_[frame_id] == 1){
      // 首次访问，且k>1，需要插入history中
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
  } else if(access_count_[frame_id] > k_){
      // 此时是已经在cache中，但是新访问了，需要把元素置于cache最开始(新)的位置
      // 置于的方法可以先删除list中的，再插入到最前头,然后更新map的value! (:
      cache_list_.erase(cache_map_[frame_id]);
      cache_list_.push_front(frame_id);
      cache_map_[frame_id] = cache_list_.begin();
  }
  // 若不是首次，同时现在访问次数还＜k，不需要更新history中的位置(FIFO原则)
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if(frame_id > static_cast<int>(replacer_size_)){
      throw std::exception();
  }
  // 需要注意的是，is_evictable中理论上存放的是0-replacer_size_个<k,v>
  // 并不是所有的k目前都存在于history或cache上
  // 只能对存在于history或cache上(即access_count>0)的做操作
  if(access_count_[frame_id] == 0){
      return ;
  }
  if(!is_evictable_[frame_id] && set_evictable){
      // 数据存在，且数据从不可被淘汰变成可被淘汰
      ++curr_size_;
  }
  if(is_evictable_[frame_id] && !set_evictable){
      // 数据存在，且数据从可被淘汰变成不可被淘汰
      --curr_size_;
  }
  is_evictable_[frame_id] = set_evictable;
}

/*
 * 这个是专门淘汰掉id为frame_id的
 * 如果id超出范围 或者 id对应的数据存在，但不可被淘汰(false) -> 抛出异常
 * 若id对应的数据不存在 -> 直接返回
 * */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if(frame_id > static_cast<int>(replacer_size_)){
      throw std::exception();
  }
  if(access_count_[frame_id] == 0){
      // 数据不存在
      return ;
  }
  if(!is_evictable_[frame_id]){
      // 存在且不可被淘汰
      throw std::exception();
  }
  // 存在且可被淘汰
  if(access_count_[frame_id] < k_){
      // 在history中
      history_list_.erase(history_map_[frame_id]);
      history_map_.erase(frame_id);
  } else {
      // 在cache中
      cache_list_.erase(cache_map_[frame_id]);
      cache_map_.erase(frame_id);
  }
  --curr_size_;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

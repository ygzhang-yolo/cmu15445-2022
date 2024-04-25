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
#include <exception>
#include <mutex>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
// Evict: 删掉Frame, 将删除的Frame id返回
// Frame要满足evictable, 且优先删除访问次数小于k的，如果都大于k, 采用LRU删除; 
// 在old代中用LRU删除, young代中用FIFO删除;
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    // 1. 空则返回false
    if(this->curr_size_ == 0) {
        return false;
    }
    // 2. 优先淘汰新生代young
    for(auto it = this->young_list_.rbegin(); it != this->young_list_.rend(); it++) {
        // 倒序遍历young, 找到第一个evictable的就是所求
        auto frame = *it;
        if(this->is_evictable_[frame]) {
            this->young_list_.erase(this->young_map_[frame]);
            this->young_map_.erase(frame);

            this->access_count_[frame] = 0;
            this->is_evictable_[frame] = false;
            --this->curr_size_;
            *frame_id = frame;
            return true;
        }
    }
    // 3. 否则淘汰老年代old
    for(auto it = this->old_list_.rbegin(); it != this->old_list_.rend(); it++) {
        // 倒序遍历young, 找到第一个evictable的就是所求
        auto frame = *it;
        if(this->is_evictable_[frame]) {
            this->old_list_.erase(this->old_map_[frame]);
            this->old_map_.erase(frame);

            this->access_count_[frame] = 0;
            this->is_evictable_[frame] = false;
            --this->curr_size_;
            *frame_id = frame;
            return true;
        }
    }
    // 4. 没有可以删除的, false
    return false;
}
// RecordAccess: 将当前timestamp给记录frame_id
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    // 1. 溢出检测, 如果超出规定的最大frame_id, 抛出异常
    if(frame_id > static_cast<int>(this->replacer_size_)) {
        throw std::exception();
    }
    // 2. 访问frame_id, acess count + 1
    ++this->access_count_[frame_id];
    // 3. 检查新生代和老生代的变动情况
    if(this->access_count_[frame_id] == this->k_) {
        // 3.1 检查访问次数是否满足k, 从新生代->老年代
        this->old_list_.push_front(frame_id);
        this->old_map_[frame_id] = this->old_list_.begin();

        if(this->young_map_.count(frame_id) >= 1) {
            this->young_list_.erase(this->young_map_[frame_id]);
            this->young_map_.erase(frame_id);
        }
    }else if(this->access_count_[frame_id] == 1) {
        // 3.2 首次访问, 直接插入新生代
        this->young_list_.push_front(frame_id);
        this->young_map_[frame_id] = this->young_list_.begin();
    }else if(this->access_count_[frame_id] > k_){
        // 3.3 访问次数超过k, 按照LRU规则，把原本frame所在的位置, 提到最开始
        this->old_list_.erase(this->old_map_[frame_id]);
        this->old_list_.push_front(frame_id);
        this->old_map_[frame_id] = this->old_list_.begin();
    }else {
        // 3.4 如果是访问次数 < k的，什么都不需要做，因为是按照FIFO原则处理young;
    }
}
// SetEvictable: 将对应frame的状态设置为set_evictable;
// 注意 curr_size的数量等于evictable, 因此如果frame状态变化要变更cur_size的数量
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    // 1. 溢出检测, 如果超出规定的最大frame_id, 抛出异常
    if(frame_id > static_cast<int>(this->replacer_size_)) {
        throw std::exception();
    }
    // 2. 如果不存在对应的frame, false
    if(this->access_count_[frame_id] == 0) {
        return;
    }
    // 3. curr_size等于evictable的数量, 因此要变动
    if(!this->is_evictable_[frame_id] && set_evictable) {
        ++this->curr_size_;
    }
    if(this->is_evictable_[frame_id] && !set_evictable) {
        --this->curr_size_;
    }
    this->is_evictable_[frame_id] = set_evictable;
}
// Remove: 淘汰frame, 删除其access历史, 减少replcer's size
// 与Evict不同的是, Remove是强制删除, 不需要做任何检查，删了就行
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    // 1. 溢出检测, 如果超出规定的最大frame_id, 抛出异常
    if(frame_id > static_cast<int>(this->replacer_size_)) {
        throw std::exception();
    }
    // 2. 如果frame不存在, 直接return
    if(this->access_count_[frame_id] == 0) {
        return;
    }
    // 3. 如果frame不是evictable的, 抛出异常
    if(!this->is_evictable_[frame_id]) {
        throw std::exception();
    }
    // 4. 找到frame的位置并淘汰
    if(this->access_count_[frame_id] < this->k_) {
        // 去young中淘汰
        this->young_list_.erase(this->young_map_[frame_id]);
        this->young_map_.erase(frame_id);
    }else {
        // 去old中淘汰
        this->old_list_.erase(this->old_map_[frame_id]);
        this->old_map_.erase(frame_id); 
    }
    // 4. 删除access, 减少replacer's size
    --this->curr_size_;
    this->access_count_[frame_id] = 0;
    this->is_evictable_[frame_id] = false;
}
//size: 返回curr_size_即可
auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock<std::mutex> lock(this->latch_);
    return this->curr_size_;
}

}  // namespace bustub

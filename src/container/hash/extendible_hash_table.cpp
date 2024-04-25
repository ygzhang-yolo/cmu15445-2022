//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {
// ExtendibleHashTable构造函数: 根据指定的bucket_size构造一个bucket;
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      this->dir_.emplace_back(std::make_shared<Bucket>(Bucket{bucket_size}));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
// Find: 查找key对应的value
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  auto index = this->IndexOf(key);      // 1. IndexOf找到key所在的bucket, 根据最后几位定位
  auto bucket_ptr = this->dir_[index];  
  return bucket_ptr->Find(key, value);  // 2. 用bucket的Find, 定位kv
}
// Remove: 删除指定的key
// TODO(ubuntu): 没有实现Shrink和Combination
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  auto bucket_ptr = this->dir_[this->IndexOf(key)]; //同Find
  return bucket_ptr->Remove(key);
}
// Insert: 插入一个kv到哈希表中
//
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  auto bucket_ptr = this->dir_[this->IndexOf(key)];
  // 1. 如果key存在, 直接更新
  for(auto &p : bucket_ptr->GetItems()) {
    if(p.first == key) {
      p.second = value;
      return;
    }
  }
  // 2. 如果bucket满了, 在插入之前要扩容
  while(this->dir_[this->IndexOf(key)]->IsFull()) {
    auto next_bucket_ptr = this->dir_[this->IndexOf(key)];
    // 2.1 如果local depth == global depth, 增加global depth, 并扩容dir为2倍
    if(next_bucket_ptr->GetDepth() == this->GetGlobalDepthInternal()) {
      ++this->global_depth_;
      int cap = this->dir_.size();
      this->dir_.resize(cap << 1);  
      for(int i = 0; i < cap; i++) {
        this->dir_[i + cap] = this->dir_[i];  //tips:保证扩容的原来的部分指向相同, 比如加入扩容后的global_depth=3; 则110和010要指向相同
      }
    }
    // 2.2 新bucket的local depth+1
    int local_depth = next_bucket_ptr->GetDepth() + 1;
    auto bucket0_ptr = std::make_shared<Bucket>(this->bucket_size_, local_depth);
    auto bucket1_ptr = std::make_shared<Bucket>(this->bucket_size_, local_depth);
    // 2.3 Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
    // 重新计算key的指向, 计算新mask下, 属于bucket1还是bucket0
    int new_mask = 1 << next_bucket_ptr->GetDepth();
    for (auto &p : next_bucket_ptr->GetItems()) {
      auto key = std::hash<K>()(p.first);   //tips: 创建hash<K>()的临时对象, 调用其()方法来进行哈希
      if (key & new_mask) {
        bucket1_ptr->Insert(p.first, p.second);  // 1
      } else {
        bucket0_ptr->Insert(p.first, p.second);  // 0
      }
    }
    ++this->num_buckets_;
    // 重新分配dir, 110和010要分别指向bucket1, bucket0;
    for(size_t i = 0; i < this->dir_.size(); ++i) {
      if(this->dir_[i] == next_bucket_ptr) {
        if((i & new_mask)) {
          this->dir_[i] = bucket1_ptr;
        }else {
          this->dir_[i] = bucket0_ptr;
        }
      }
    }
  }
  // 3. 重新获取bucket, 并插入数据
  bucket_ptr = this->dir_[this->IndexOf(key)];
  bucket_ptr->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {} //构造函数直接用默认
// Find: 遍历bucket找是否存在key，一个遍历即可没什么需要理解的
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto &p : this->list_) {
    if(p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}
// Remove: 直接删除key所在的位置；如果key不存在, 返回false;
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = this->list_.begin();
  while(it != this->list_.end() && it->first != key) {it++;}  //遍历找key的位置
  if(it == this->list_.end()) {
    return false; //key不存在, 返回false;
  }
  this->list_.erase(it);  //删除key->iterator并返回true
  return true;
}
// Insert: 将key插入bucket
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // TODO(ubuntu): 我交换1,2步的顺序, 是否有潜在的bug, 我觉得这样是更符合逻辑的
  // 1. 如果key已经存在, 更新value;
  for(auto &p : list_) {
    if(p.first == key) {
      p.second = value;
      return true;
    }
  }
  // 2. 如果bucket满了, 不插入并返回false
  if(this->IsFull()) {
    return false;
  }
  // 3. 不存在key, 且bucket不满, 插入
  this->list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

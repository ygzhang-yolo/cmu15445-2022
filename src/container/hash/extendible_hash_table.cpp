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
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // emplace_back减少一次拷贝，提高性能
  dir_.emplace_back(std::make_shared<Bucket>(Bucket(bucket_size)));
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

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key); // 生成hash索引
  auto bucketPtr = dir_[index]; // 一定不会不存在，index一定<dir_.size()
  return bucketPtr->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucketPtr = dir_[index];
  return bucketPtr->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucketPtr = dir_[index];
  if(!bucketPtr->IsFull()){
    // 未满，则更新还是直接插入都可以，直接Insert
    bucketPtr->Insert(key,value);
    return ;
  }
  // 满了，要判断是否存在key，存在则直接更新，不存在要拆分
  while(dir_[IndexOf(key)]->IsFull()){
    auto newIndex = IndexOf(key);
    auto nextBucketPtr = dir_[newIndex];
    for(auto &k : nextBucketPtr->GetItems()){
      if(k.first == key){
        // 存在key，更新而不用插入
        k.second = value;
        return ;
      }
    }
    // 不存在，在满的情况下需要插入
    if(nextBucketPtr->GetDepth() == GetGlobalDepthInternal()){
      //case1: localDepth == GlobalDepth，需要global depth++
      // 扩容dir_(二倍)
      // 然后将bucket重新分配入扩容后的dir_中，被指向的index变为1x与0x两个，即增加一个1<<global为index或x+capacity
      // 要插入的这个bucket需要split，上面的两个指针要分别指向两个新的bucket，同时将其中所有key重新计算index，分配入两个新的bucket中
      ++global_depth_;
      int capacity = dir_.size();
      dir_.resize(capacity << 1); // 扩容
      // 新增指针指向bucket
      for(int i = 0; i < capacity; ++i){
        dir_[i+capacity] = dir_[i];
      }
    }
    // 现在已经分割好(即local depth < global depth)，插入的地方一定会满，需要先split
    // split bucket
    auto bucket0Ptr = std::make_shared<Bucket>(bucket_size_,nextBucketPtr->GetDepth()+1);
    auto bucket1Ptr = std::make_shared<Bucket>(bucket_size_,nextBucketPtr->GetDepth()+1);
    // 重新计算原bucket中的key，分配到两个新的bucket中
    int newMask = 1 << nextBucketPtr->GetDepth(); //原本是最右2位(local depth)，现在要看第3位是0还是1了
    for(auto &item : nextBucketPtr->GetItems()){
      auto itemHashKey = std::hash<K>()(item.first);
      if((itemHashKey & newMask)){
        // 1
        bucket1Ptr->Insert(item.first,item.second);
      } else{
        // 0
        bucket0Ptr->Insert(item.first,item.second);
      }
    }
    ++num_buckets_;
    // 将两个新的Bucket分配到对应的dir_中(即newBucketPtr所在两个位置)
    for(size_t i = 0; i < dir_.size(); ++i){
      if(dir_[i] == nextBucketPtr){
        // 找到了newBucketPtr所在位置
        if((i & newMask)){
          dir_[i] = bucket1Ptr;
        } else{
          dir_[i] = bucket0Ptr;
        }
      }
    }
  }
  // 现在已经分割好，直接insert即可
  auto finalIndex = IndexOf(key);
  auto finalBucketPtr = dir_[finalIndex];
  finalBucketPtr->Insert(key,value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto k : list_){
    if(k.first == key){
      value = k.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {

  auto k = list_.begin();
  while(k != list_.end() && k->first != key){k++;}
  if(k == list_.end()){
    return false;
  }
  // 找到了，就是当前指向的位置，删除
  list_.erase(k);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if(IsFull()){
    // 满了不能直接插入
    return false;
  }
  for(auto k : list_){
    if(k.first == key){
      k.second = value;
      return true;
    }
  }
  // 没有且未满，插入
  list_.emplace_back(key,value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

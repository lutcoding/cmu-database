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
  dir_ = {std::shared_ptr<Bucket>(new Bucket(bucket_size, 0))};
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
  latch_.lock();
  auto ok = dir_[IndexOf(key)]->Find(key, value);
  latch_.unlock();
  return ok;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  auto ok = dir_[IndexOf(key)]->Remove(key);
  latch_.unlock();

  return ok;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  auto index = IndexOf(key);
  std::shared_ptr<Bucket> bucket(dir_[index]);

  while (!bucket->Insert(key, value)) {
    if (bucket->GetDepth() < global_depth_) {
      auto last_depth = bucket->GetDepth();

      bucket->IncrementDepth();
      std::shared_ptr<Bucket> split_bucket(new Bucket(bucket_size_, bucket->GetDepth()));
      std::list<std::pair<K, V>> &lists = bucket->GetItems();

      auto last_hash = IndexOfDepth(lists.begin()->first, last_depth);
      auto interval = 1 << bucket->GetDepth();
      auto next_hash = last_hash + interval / 2;
      // re-hash the key
      for (auto i = lists.begin(); i != lists.end();) {
        if (IndexOfDepth(i->first, bucket->GetDepth()) == next_hash) {
          split_bucket->Insert(i->first, i->second);
          i = lists.erase(i);
          continue;
        }
        ++i;
      }
      for (typename std::vector<std::shared_ptr<Bucket>>::size_type i = next_hash; i < dir_.size(); i += interval) {
        dir_[i] = split_bucket;
      }
      num_buckets_++;
    } else {
      auto last_depth = global_depth_;
      dir_.resize(dir_.size() * 2);
      num_buckets_++;
      global_depth_++;
      bucket->IncrementDepth();
      std::shared_ptr<Bucket> split_bucket(new Bucket(bucket_size_, bucket->GetDepth()));
      std::list<std::pair<K, V>> &lists = bucket->GetItems();

      auto split_hash = index + (1 << last_depth);
      for (auto i = lists.begin(); i != lists.end();) {
        if (IndexOf(i->first) == split_hash) {
          split_bucket->Insert(i->first, i->second);
          i = lists.erase(i);
          continue;
        }
        ++i;
      }
      dir_[split_hash] = split_bucket;
      for (typename std::vector<std::shared_ptr<Bucket>>::size_type i = (1 << last_depth); i < dir_.size(); ++i) {
        if (i == split_hash) {
          continue;
        }
        dir_[i] = dir_[IndexOfIntDepth(static_cast<int>(i), last_depth)];
      }
    }
    bucket = dir_[IndexOf(key)];
  }
  latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto i = list_.begin(); i != list_.end(); ++i) {
    if (i->first == key) {
      list_.erase(i);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto ok = Remove(key);
  if (!ok && this->IsFull()) {
    return false;
  }
  list_.push_back(std::pair<K, V>(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  auto iter = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAfter(const KeyType &key, const ValueType &value, const ValueType &old) {
  int index = ValueIndex(old) + 1;
  std::move_backward(std::begin(array_) + index, std::begin(array_) + GetSize(), std::begin(array_) + GetSize() + 1);
  array_[index] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  auto iter = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                               [&comparator](const auto &pair1, auto key) { return comparator(pair1.first, key) < 0; });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindChild(const KeyType &key, const KeyComparator &comparator) -> ValueType {
  int index = FindIndex(key, comparator);
  if (index == GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(array_[index].first, key) == 0) {
    return array_[index].second;
  }
  return array_[index - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *new_internal) {
  int interval_index = GetSize() / 2;
  for (int i = interval_index, j = 0; i < GetSize(); i++, j++) {
    new_internal->SetKeyAt(j, array_[i].first);
    new_internal->SetValueAt(j, array_[i].second);
  }
  new_internal->SetSize(GetSize() - interval_index);
  SetSize(interval_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReDirectChild(bustub::BufferPoolManager *buffer_pool_manager) {
  page_id_t id = GetPageId();
  for (int i = 0; i < GetSize(); ++i) {
    auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[i].second)->GetData());
    page->SetParentPageId(id);
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetRSibling(int index) -> page_id_t {
  if (index == GetSize() - 1) {
    return INVALID_PAGE_ID;
  }
  return array_[index + 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLSibling(int index) -> page_id_t {
  if (index == 0) {
    return INVALID_PAGE_ID;
  }
  return array_[index - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::HalfFull() -> bool { return GetSize() > GetMinSize(); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, BPlusTreeInternalPage *page,
                                           BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = {key, page->ValueAt(0)};
  IncreaseSize(1);
  BPlusTreePage *r_page;
  for (int i = 1, j = GetSize(); i < page->GetSize(); ++i, ++j) {
    array_[j] = {page->KeyAt(i), page->ValueAt(i)};
  }
  IncreaseSize(page->GetSize() - 1);
  for (int i = 0; i < page->GetSize(); ++i) {
    r_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(page->ValueAt(i))->GetData());
    r_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->ValueAt(i), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteByIndex(const int &index) {
  std::move(std::begin(array_) + index + 1, std::begin(array_) + GetSize(), std::begin(array_) + index);
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertByIndex(const int &index, const KeyType &key, const ValueType &value) {
  std::move_backward(std::begin(array_) + index, std::begin(array_) + GetSize(), std::begin(array_) + GetSize() + 1);
  array_[index] = {key, value};
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

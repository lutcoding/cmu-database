/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, int index,
                                  BufferPoolManager *buffer_pool_manager, page_id_t id)
    : leaf_(leaf), buffer_pool_manager_(buffer_pool_manager), index_(index), page_id_(id) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetPair(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (index_ >= leaf_->GetSize()) {
    auto next = leaf_->GetNextPageId();
    Unpin();
    if (next == INVALID_PAGE_ID) {
      return *this;
    }
    auto *page = buffer_pool_manager_->FetchPage(next);
    page->RLatch();
    leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    page_id_ = page->GetPageId();
    index_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return page_id_ != itr.page_id_ || index_ != itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Unpin() {
  buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

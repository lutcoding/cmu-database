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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteSet(bustub::Transaction *transaction) {
  auto set = transaction->GetDeletedPageSet();
  for (auto i : *set) {
    buffer_pool_manager_->DeletePage(i);
  }
  set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLockSet(bustub::Transaction *transaction, bool is_dirty, int operation) {
  auto set = transaction->GetPageSet();
  for (auto i : *set) {
    if (reinterpret_cast<BPlusTreePage *>(i->GetData())->IsRootPage()) {
      latch_.unlock();
    }
    if (operation == 0) {
      i->RUnlatch();
    } else {
      i->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(i->GetPageId(), is_dirty);
  }
  set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetConcurrentLeaf(const KeyType &key, int operation, Transaction *transaction) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (operation == 0) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  transaction->AddIntoPageSet(page);
  auto *btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!btree_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(btree_page);
    page_id_t child_id = internal_page->FindChild(key, comparator_);
    auto *child_page = buffer_pool_manager_->FetchPage(child_id);
    if (operation == 0) {
      child_page->RLatch();
      ReleaseLockSet(transaction, false, 0);
      btree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    } else if (operation == 1) {
      child_page->WLatch();
      btree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      if (!btree_page->WillOverFlow()) {
        ReleaseLockSet(transaction, false, 1);
      }
    } else {
      child_page->WLatch();
      btree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      if (!btree_page->WillUnderFlow()) {
        ReleaseLockSet(transaction, false, 2);
      }
    }
    transaction->AddIntoPageSet(child_page);
    page = child_page;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page, KeyType &key) -> BPlusTreePage * {
  page_id_t new_page_id;
  if (page->IsLeafPage()) {
    auto *old = reinterpret_cast<LeafPage *>(page);
    auto *newp = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
    newp->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    old->Split(newp);
    newp->SetNextPageId(old->GetNextPageId());
    old->SetNextPageId(new_page_id);
    key = newp->KeyAt(0);
    return newp;
  }
  auto *old = reinterpret_cast<InternalPage *>(page);
  auto *newp = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
  newp->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  old->Split(newp);
  newp->ReDirectChild(buffer_pool_manager_);
  key = newp->KeyAt(0);
  return newp;
}

/*
 * Helper function to solve the overflow within the node
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SolveOverFlow(Transaction *transaction) {
  auto set = transaction->GetPageSet();
  for (auto i = set->rbegin(); i != set->rend(); ++i) {
    auto *old = reinterpret_cast<BPlusTreePage *>((*i)->GetData());
    if (!old->IsOverFlow()) {
      break;
    }
    KeyType first_key;
    auto *newp = Split(old, first_key);
    if (old->IsRootPage()) {
      auto *r = buffer_pool_manager_->NewPage(&root_page_id_);
      auto *root = reinterpret_cast<InternalPage *>(r->GetData());
      root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      root->SetKeyAt(1, first_key);
      root->SetValueAt(0, old->GetPageId());
      root->SetValueAt(1, newp->GetPageId());
      root->SetSize(2);

      old->SetParentPageId(root_page_id_);
      newp->SetParentPageId(root_page_id_);

      buffer_pool_manager_->UnpinPage(newp->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);

      UpdateRootPageId(root_page_id_);
      latch_.unlock();
      break;
    }
    auto *parent = reinterpret_cast<InternalPage *>((*(i + 1))->GetData());
    parent->InsertAfter(first_key, newp->GetPageId(), old->GetPageId());
    newp->SetParentPageId(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(newp->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SolveUnderFlow(Transaction *transaction) {
  auto set = transaction->GetPageSet();
  for (auto j = set->rbegin() + 1; j != set->rend(); ++j) {
    auto *page = reinterpret_cast<InternalPage *>((*j)->GetData());
    if (!page->IsUnderFlow() || page->IsRootPage()) {
      return;
    }
    auto *parent = reinterpret_cast<InternalPage *>((*(j + 1))->GetData());
    int index = parent->ValueIndex(page->GetPageId());
    page_id_t r_sibling = parent->GetRSibling(index);
    page_id_t l_sibling = parent->GetLSibling(index);
    InternalPage *r_sibling_page;
    InternalPage *l_sibling_page;
    if (r_sibling != INVALID_PAGE_ID) {
      auto *r = buffer_pool_manager_->FetchPage(r_sibling);
      r->WLatch();
      r_sibling_page = reinterpret_cast<InternalPage *>(r->GetData());
      if (r_sibling_page->HalfFull()) {
        page->InsertByIndex(page->GetSize(), parent->KeyAt(index + 1), r_sibling_page->ValueAt(0));
        auto *redirect_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(r_sibling_page->ValueAt(0))->GetData());
        redirect_page->SetParentPageId(page->GetPageId());
        parent->SetKeyAt(index + 1, r_sibling_page->KeyAt(1));
        for (int i = 1; i < r_sibling_page->GetSize(); ++i) {
          r_sibling_page->SetValueAt(i - 1, r_sibling_page->ValueAt(i));
          r_sibling_page->SetKeyAt(i - 1, r_sibling_page->KeyAt(i));
        }
        r_sibling_page->DecreaseSize(1);
        r->WUnlatch();
        buffer_pool_manager_->UnpinPage(r_sibling, true);
        buffer_pool_manager_->UnpinPage(redirect_page->GetPageId(), true);
        return;
      }
      r->WUnlatch();
      buffer_pool_manager_->UnpinPage(r_sibling, false);
    }
    if (l_sibling != INVALID_PAGE_ID) {
      auto *l = buffer_pool_manager_->FetchPage(l_sibling);
      l->WLatch();
      l_sibling_page = reinterpret_cast<InternalPage *>(l->GetData());
      if (l_sibling_page->HalfFull()) {
        for (int i = page->GetSize() - 1; i >= 1; --i) {
          page->SetKeyAt(i + 1, page->KeyAt(i));
          page->SetValueAt(i + 1, page->ValueAt(i));
        }
        page->SetValueAt(1, page->ValueAt(0));
        page->SetKeyAt(1, parent->KeyAt(index));
        page->SetValueAt(0, l_sibling_page->ValueAt(l_sibling_page->GetSize() - 1));
        parent->SetKeyAt(index, l_sibling_page->KeyAt(l_sibling_page->GetSize() - 1));
        page->IncreaseSize(1);
        auto *redirect_page = reinterpret_cast<BPlusTreePage *>(
            buffer_pool_manager_->FetchPage(l_sibling_page->ValueAt(l_sibling_page->GetSize() - 1))->GetData());
        redirect_page->SetParentPageId(page->GetPageId());
        l_sibling_page->DecreaseSize(1);
        l->WUnlatch();
        buffer_pool_manager_->UnpinPage(l_sibling, true);
        buffer_pool_manager_->UnpinPage(redirect_page->GetPageId(), true);
        return;
      }
      l->WUnlatch();
      buffer_pool_manager_->UnpinPage(l_sibling, false);
    }
    if (r_sibling != INVALID_PAGE_ID) {
      transaction->AddIntoDeletedPageSet(r_sibling);
      r_sibling_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(r_sibling)->GetData());
      page->Merge(parent->KeyAt(index + 1), r_sibling_page, buffer_pool_manager_);
      parent->DeleteByIndex(index + 1);
      if (parent->GetSize() == 1 && parent->IsRootPage()) {
        transaction->AddIntoDeletedPageSet(parent->GetPageId());
        UpdateRootPageId(page->GetPageId());
        root_page_id_ = page->GetPageId();
        page->SetParentPageId(INVALID_PAGE_ID);
        (*(transaction->GetPageSet()->begin()))->WUnlatch();
        transaction->GetPageSet()->pop_front();
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(r_sibling, true);
        return;
      }
      buffer_pool_manager_->UnpinPage(r_sibling, true);
      continue;
    }
    transaction->AddIntoDeletedPageSet(page->GetPageId());
    l_sibling_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(l_sibling)->GetData());
    l_sibling_page->Merge(parent->KeyAt(index), page, buffer_pool_manager_);
    parent->DecreaseSize(1);
    if (parent->GetSize() == 1 && parent->IsRootPage()) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
      UpdateRootPageId(l_sibling);
      root_page_id_ = l_sibling;
      l_sibling_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(l_sibling, true);
      return;
    }
    buffer_pool_manager_->UnpinPage(l_sibling, true);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  latch_.lock();
  if (IsEmpty()) {
    latch_.unlock();
    return false;
  }
  transaction = new Transaction(0);
  auto *leaf = reinterpret_cast<LeafPage *>(GetConcurrentLeaf(key, 0, transaction)->GetData());
  int index = leaf->FindIndex(key, comparator_);
  bool ok = true;
  if (index == leaf->GetSize() || comparator_(leaf->KeyAt(index), key) != 0) {
    ok = false;
  } else {
    result->push_back(leaf->ValueAt(index));
  }
  ReleaseLockSet(transaction, false, 0);
  delete transaction;
  return ok;
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
  latch_.lock();
  if (IsEmpty()) {
    auto *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->InsertByIndex(0, key, value);
    UpdateRootPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    latch_.unlock();
    return true;
  }
  auto *l = reinterpret_cast<LeafPage *>(GetConcurrentLeaf(key, 1, transaction)->GetData());
  if (!l->Insert(key, value, comparator_)) {
    ReleaseLockSet(transaction, false, 1);
    return false;
  }
  SolveOverFlow(transaction);
  ReleaseLockSet(transaction, true, 1);
  return true;
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
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  latch_.lock();
  if (IsEmpty()) {
    latch_.unlock();
    return;
  }
  Page *leaf = GetConcurrentLeaf(key, 2, transaction);

  auto *leaf_page = reinterpret_cast<LeafPage *>(leaf->GetData());
  int delete_index = leaf_page->Delete(key, comparator_);
  // 若leaf中不含key, 则internal必为nullptr
  if (delete_index == -1 || leaf_page->IsRootPage()) {
    ReleaseLockSet(transaction, delete_index != -1, 2);
    return;
  }
  if (!leaf_page->IsUnderFlow()) {
    ReleaseLockSet(transaction, true, 2);
    return;
  }
  auto *parent = reinterpret_cast<InternalPage *>(*(transaction->GetPageSet()->rbegin() + 1));
  int key_index_in_parent = parent->ValueIndex(leaf->GetPageId());
  page_id_t r_sibling = parent->GetRSibling(key_index_in_parent);
  page_id_t l_sibling = parent->GetLSibling(key_index_in_parent);
  LeafPage *r_sibling_page;
  LeafPage *l_sibling_page;
  if (r_sibling != INVALID_PAGE_ID) {
    auto *r = buffer_pool_manager_->FetchPage(r_sibling);
    r->WLatch();
    r_sibling_page = reinterpret_cast<LeafPage *>(r->GetData());
    if (r_sibling_page->HalfFull()) {
      leaf_page->InsertByIndex(leaf_page->GetSize(), r_sibling_page->KeyAt(0), r_sibling_page->ValueAt(0));
      parent->SetKeyAt(key_index_in_parent + 1, r_sibling_page->KeyAt(1));
      r_sibling_page->DeleteByIndex(0);
      r->WUnlatch();
      buffer_pool_manager_->UnpinPage(r_sibling, true);
      ReleaseLockSet(transaction, true, 2);
      return;
    }
    r->WUnlatch();
    buffer_pool_manager_->UnpinPage(r_sibling, false);
  }
  if (l_sibling != INVALID_PAGE_ID) {
    auto *l = buffer_pool_manager_->FetchPage(l_sibling);
    l->WLatch();
    l_sibling_page = reinterpret_cast<LeafPage *>(l->GetData());
    if (l_sibling_page->HalfFull()) {
      parent->SetKeyAt(key_index_in_parent, l_sibling_page->KeyAt(l_sibling_page->GetSize() - 1));
      leaf_page->InsertByIndex(0, l_sibling_page->KeyAt(l_sibling_page->GetSize() - 1),
                               l_sibling_page->ValueAt(l_sibling_page->GetSize() - 1));
      l_sibling_page->DecreaseSize(1);
      l->WUnlatch();
      buffer_pool_manager_->UnpinPage(l_sibling, true);
      ReleaseLockSet(transaction, true, 2);
      return;
    }
    l->WUnlatch();
    buffer_pool_manager_->UnpinPage(l_sibling, false);
  }
  // 合并右节点
  if (r_sibling != INVALID_PAGE_ID) {
    transaction->AddIntoDeletedPageSet(r_sibling);
    auto *r = buffer_pool_manager_->FetchPage(r_sibling);
    r_sibling_page = reinterpret_cast<LeafPage *>(r->GetData());
    leaf_page->Merge(r_sibling_page);
    leaf_page->SetNextPageId(r_sibling_page->GetNextPageId());
    parent->DeleteByIndex(key_index_in_parent + 1);
    if (parent->IsRootPage() && parent->GetSize() == 1) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
      leaf_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(leaf_page->GetPageId());
      root_page_id_ = leaf_page->GetPageId();
      (*(transaction->GetPageSet()->begin()))->WUnlatch();
      transaction->GetPageSet()->pop_front();
      buffer_pool_manager_->UnpinPage(r_sibling, true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      ReleaseLockSet(transaction, true, 2);
      DeleteSet(transaction);
      return;
    }
    buffer_pool_manager_->UnpinPage(r_sibling, true);
    SolveUnderFlow(transaction);
    ReleaseLockSet(transaction, true, 2);
    DeleteSet(transaction);
    return;
  }
  // 合并左节点，此时必为某子树的最右leaf节点
  transaction->AddIntoDeletedPageSet(leaf->GetPageId());
  auto *l = buffer_pool_manager_->FetchPage(l_sibling);
  l_sibling_page = reinterpret_cast<LeafPage *>(l->GetData());
  l_sibling_page->Merge(leaf_page);
  parent->DecreaseSize(1);
  // l_sibling_page->SetNextPageId(INVALID_PAGE_ID);
  l_sibling_page->SetNextPageId(leaf_page->GetNextPageId());
  if (parent->IsRootPage() && parent->GetSize() == 1) {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
    l_sibling_page->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(l_sibling_page->GetPageId());
    root_page_id_ = l_sibling_page->GetPageId();
    buffer_pool_manager_->UnpinPage(l_sibling, true);
    ReleaseLockSet(transaction, true, 2);
    DeleteSet(transaction);
    return;
  }
  buffer_pool_manager_->UnpinPage(l_sibling, true);
  SolveUnderFlow(transaction);
  ReleaseLockSet(transaction, true, 2);
  DeleteSet(transaction);
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
  latch_.lock();
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->RLatch();
  latch_.unlock();
  auto *btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!btree_page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(btree_page);
    auto *child = buffer_pool_manager_->FetchPage(internal->ValueAt(0));
    child->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child;
    btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_, leaf->GetPageId());
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto *transaction = new Transaction(0);
  latch_.lock();
  auto *leaf = reinterpret_cast<LeafPage *>(GetConcurrentLeaf(key, 0, transaction)->GetData());
  int index = leaf->FindIndex(key, comparator_);
  transaction->GetPageSet()->clear();
  delete transaction;
  return INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_, leaf->GetPageId());
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->RLatch();
  auto *btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!btree_page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(btree_page);
    auto *child = buffer_pool_manager_->FetchPage(internal->ValueAt(internal->GetSize() - 1));
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child;
    page->RLatch();
    btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf, leaf->GetSize(), buffer_pool_manager_, leaf->GetPageId());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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

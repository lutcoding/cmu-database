//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "set"

namespace bustub {

void LockManager::CheckUpdateState(Transaction *txn, LockMode lock_mode) {
  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
    }
  }
}

void LockManager::ThrowException(Transaction *txn, AbortReason reason) {
  AbortTransaction(txn);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

void LockManager::AbortTransaction(Transaction *txn) { txn->SetState(TransactionState::ABORTED); }

void LockManager::ReleaseTxnInWaitQueue(txn_id_t id, std::shared_ptr<LockRequestQueue> &queue) {
  for (auto i = queue->wait_queue_.begin(); i != queue->wait_queue_.end();) {
    if ((*i)->txn_id_ == id) {
      if (queue->upgrading_ == id) {
        queue->upgrading_ = INVALID_TXN_ID;
      }
      i = queue->grant_queue_.erase(i);
    } else {
      ++i;
    }
  }
  queue->cv_.notify_all();
}

void LockManager::CheckTableLock(Transaction *txn, LockMode lock_mode) { CheckIsolationLevel(txn, lock_mode); }

void LockManager::CheckRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    ThrowException(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::SHRINKING && lock_mode == LockMode::EXCLUSIVE) {
    ThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
  }
  CheckIsolationLevel(txn, lock_mode);
  if (lock_mode == LockMode::SHARED && !txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
      !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
      !txn->IsTableIntentionExclusiveLocked(oid)) {
    ThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  if (lock_mode == LockMode::EXCLUSIVE && !txn->IsTableExclusiveLocked(oid) &&
      !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    ThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
}

void LockManager::CheckIsolationLevel(Transaction *txn, LockMode lock_mode) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode != LockMode::EXCLUSIVE &&
      lock_mode != LockMode::INTENTION_EXCLUSIVE) {
    ThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    bool flag{false};
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
        txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      flag = true;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      flag = true;
    }
    if (flag) {
      ThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
    }
  }
}

void LockManager::InsertTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

void LockManager::DeleteTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

void LockManager::InsertRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
}

void LockManager::DeleteRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
}

auto LockManager::UpGradeLockMode(LockMode l, LockMode r) -> bool {
  if (l == LockMode::INTENTION_SHARED) {
    return r != LockMode::INTENTION_SHARED;
  }
  if (l == LockMode::SHARED) {
    return r == LockMode::EXCLUSIVE || r == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (l == LockMode::INTENTION_EXCLUSIVE) {
    return r == LockMode::EXCLUSIVE || r == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (l == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return r == LockMode::EXCLUSIVE;
  }
  return false;
}

auto LockManager::CompatibleLockMode(LockManager::LockMode l, LockMode r) -> bool {
  switch (l) {
    case LockMode::INTENTION_SHARED:
      return r != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return r == LockMode::INTENTION_SHARED || r == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
      return r == LockMode::INTENTION_SHARED || r == LockMode::SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return r == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
  }
}

auto LockManager::CheckUpgrade(Transaction *txn, const std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode,
                               std::unique_lock<std::mutex> &lck, const table_oid_t &oid, bool is_table, const RID &rid)
    -> bool {
  bool need_upgrade{false};
  std::list<std::shared_ptr<LockRequest>>::iterator iter;
  for (auto i = queue->grant_queue_.begin(); i != queue->grant_queue_.end(); ++i) {
    if (i->get()->txn_id_ == txn->GetTransactionId()) {
      need_upgrade = true;
      iter = i;
      break;
    }
  }
  if (need_upgrade) {
    if ((*iter)->lock_mode_ == lock_mode) {
      lck.unlock();
      return true;
    }
    if (queue->upgrading_ != INVALID_TXN_ID) {
      lck.unlock();
      ThrowException(txn, AbortReason::UPGRADE_CONFLICT);
    } else if (!UpGradeLockMode((*iter)->lock_mode_, lock_mode)) {
      lck.unlock();
      ThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    }
    queue->upgrading_ = txn->GetTransactionId();
    if (is_table) {
      DeleteTableLock(txn, (*iter)->lock_mode_, oid);
    } else {
      DeleteRowLock(txn, (*iter)->lock_mode_, oid, rid);
    }
    iter->get()->lock_mode_ = lock_mode;
    queue->wait_queue_.emplace_front(*iter);
    queue->grant_queue_.erase(iter);
  } else {
    queue->wait_queue_.emplace_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  }
  return false;
}

auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                            std::shared_ptr<LockRequestQueue> &queue, bool is_table, RID rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    ReleaseTxnInWaitQueue(txn->GetTransactionId(), queue);
    return true;
  }
  std::list<std::shared_ptr<LockRequest>>::iterator iter;
  for (auto i = queue->wait_queue_.begin(); i != queue->wait_queue_.end(); ++i) {
    if (i->get()->txn_id_ == txn->GetTransactionId()) {
      iter = i;
      break;
    }
  }
  if (iter != queue->wait_queue_.begin()) {
    for (auto i = queue->wait_queue_.begin(); i != iter; ++i) {
      if (!CompatibleLockMode(i->get()->lock_mode_, lock_mode)) {
        return false;
      }
    }
  }
  for (auto &i : queue->grant_queue_) {
    if (!CompatibleLockMode(i->lock_mode_, lock_mode)) {
      return false;
    }
  }

  if (is_table) {
    InsertTableLock(txn, lock_mode, oid);
  } else {
    InsertRowLock(txn, lock_mode, oid, rid);
  }
  queue->grant_queue_.emplace_back(*iter);
  queue->wait_queue_.erase(iter);
  if (queue->upgrading_ == txn->GetTransactionId()) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  CheckTableLock(txn, lock_mode);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  auto queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lck(queue->latch_);
  table_lock_map_latch_.unlock();
  if (CheckUpgrade(txn, queue, lock_mode, lck, oid, true, RID())) {
    return true;
  }
  while (!GrantLock(txn, lock_mode, oid, queue, true, RID())) {
    queue->cv_.wait(lck);
  }
  lck.unlock();
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto queue = table_lock_map_[oid];
  queue->latch_.lock();
  table_lock_map_latch_.unlock();
  bool is_exist{false};
  std::list<std::shared_ptr<LockRequest>>::iterator iter;
  for (auto i = queue->grant_queue_.begin(); i != queue->grant_queue_.end(); ++i) {
    if ((*i)->txn_id_ == txn->GetTransactionId() && (*i)->oid_ == oid) {
      is_exist = true;
      iter = i;
      break;
    }
  }
  if (!is_exist) {
    queue->latch_.unlock();
    ThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    queue->latch_.unlock();
    ThrowException(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_mode = iter->get()->lock_mode_;
  queue->grant_queue_.erase(iter);
  queue->latch_.unlock();
  queue->cv_.notify_all();
  CheckUpdateState(txn, lock_mode);
  DeleteTableLock(txn, lock_mode, oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  CheckRowLock(txn, lock_mode, oid);

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lck(queue->latch_);
  row_lock_map_latch_.unlock();
  if (CheckUpgrade(txn, queue, lock_mode, lck, oid, false, rid)) {
    return true;
  }
  while (!GrantLock(txn, lock_mode, oid, queue, false, rid)) {
    queue->cv_.wait(lck);
  }
  lck.unlock();
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto queue = row_lock_map_[rid];
  queue->latch_.lock();
  row_lock_map_latch_.unlock();
  bool is_exist{false};
  std::list<std::shared_ptr<LockRequest>>::iterator iter;
  for (auto i = queue->grant_queue_.begin(); i != queue->grant_queue_.end(); ++i) {
    if ((*i)->txn_id_ == txn->GetTransactionId() && (*i)->oid_ == oid && (*i)->rid_ == rid) {
      is_exist = true;
      iter = i;
      break;
    }
  }
  if (!is_exist) {
    queue->latch_.unlock();
    ThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_mode = iter->get()->lock_mode_;
  queue->grant_queue_.erase(iter);
  queue->latch_.unlock();
  queue->cv_.notify_all();
  CheckUpdateState(txn, lock_mode);
  DeleteRowLock(txn, lock_mode, oid, rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  auto iter = waits_for_.find(t1);
  if (iter != waits_for_.end()) {
    auto list_iter = std::find(iter->second.begin(), iter->second.end(), t2);
    if (list_iter != iter->second.end()) {
      iter->second.erase(list_iter);
      if (iter->second.empty()) {
        waits_for_.erase(iter);
      }
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::scoped_lock lock(waits_for_latch_);
  std::vector<txn_id_t> v;
  txn_id_t youngest = INVALID_TXN_ID;
  v.reserve(waits_for_.size());
  bool flag{false};
  for (const auto &i : waits_for_) {
    v.emplace_back(i.first);
  }
  std::sort(v.begin(), v.end());
  for (const auto &i : v) {
    if (visit_[i]) {
      continue;
    }
    std::vector<txn_id_t> path;
    bool cycle_exist{false};
    DFSFirstCycle(i, i, cycle_exist, path);
    if (cycle_exist) {
      flag = true;
      for (const auto &j : path) {
        if (youngest == INVALID_TXN_ID || j > youngest) {
          youngest = j;
        }
      }
      *txn_id = youngest;
      break;
    }
  }
  visit_.clear();
  return flag;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  std::scoped_lock lock(waits_for_latch_);
  for (const auto &i : waits_for_) {
    for (const auto &j : i.second) {
      edges.emplace_back(std::pair<txn_id_t, txn_id_t>(i.first, j));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_latch_.lock();
      BuildGraph();
      std::vector<txn_id_t> v;
      v.reserve(waits_for_.size());
      for (const auto &i : waits_for_) {
        v.emplace_back(i.first);
      }
      std::sort(v.begin(), v.end());
      for (const auto &i : v) {
        if (visit_[i]) {
          continue;
        }
        std::vector<txn_id_t> path;
        DFS(i, i, path);
      }
      for (const auto &i : abort_) {
        if (i.second) {
          Transaction *txn = TransactionManager::GetTransaction(i.first);
          AbortTransaction(txn);
          Notify(i.first);
        }
      }
      waits_for_.clear();
      visit_.clear();
      abort_.clear();
      waits_for_latch_.unlock();
    }
  }
}

void LockManager::BuildGraph() {
  table_lock_map_latch_.lock();
  for (const auto &i : table_lock_map_) {
    InsertEdgeToGraph(i.second);
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (const auto &i : row_lock_map_) {
    InsertEdgeToGraph(i.second);
  }
  row_lock_map_latch_.unlock();
}

void LockManager::InsertEdgeToGraph(const std::shared_ptr<LockRequestQueue> &queue) {
  std::scoped_lock lock(queue->latch_);
  if (queue->grant_queue_.empty() || queue->wait_queue_.empty()) {
    return;
  }
  for (const auto &j : queue->wait_queue_) {
    if (TransactionManager::GetTransaction(j->txn_id_)->GetState() == TransactionState::ABORTED) {
      continue;
    }
    for (const auto &k : queue->grant_queue_) {
      if (TransactionManager::GetTransaction(k->txn_id_)->GetState() != TransactionState::ABORTED) {
        waits_for_[j->txn_id_].insert(k->txn_id_);
      }
    }
  }
}

void LockManager::DFS(txn_id_t begin, txn_id_t now, std::vector<txn_id_t> &path) {
  path.emplace_back(now);
  visit_[now] = true;
  // if now has no neighbors, must not be a participant in deadlock
  if (waits_for_.find(now) == waits_for_.end()) {
    visit_[now] = false;
    path.pop_back();
    return;
  }
  for (const auto &i : waits_for_[now]) {
    if (i == begin) {
      // find the youngest
      txn_id_t t = begin;
      for (auto &j : path) {
        if (j > t) {
          t = j;
        }
      }
      abort_[t] = true;
      path.pop_back();
      if (t != now) {
        visit_[now] = false;
      }
      return;
    }
    if (visit_[i]) {
      continue;
    }
    DFS(begin, i, path);
    for (int &j : path) {
      if (abort_[j]) {
        path.pop_back();
        if (j != now) {
          visit_[now] = false;
        }
        return;
      }
    }
  }
  path.pop_back();
  if (!abort_[now]) {
    visit_[now] = false;
  }
}

void LockManager::Notify(txn_id_t id) {
  table_lock_map_latch_.lock();
  for (auto &i : table_lock_map_) {
    i.second->latch_.lock();
    ReleaseTxnInWaitQueue(id, i.second);
    i.second->latch_.unlock();
    i.second->cv_.notify_all();
  }
  table_lock_map_latch_.unlock();
  row_lock_map_latch_.lock();
  for (auto &i : row_lock_map_) {
    i.second->latch_.lock();
    ReleaseTxnInWaitQueue(id, i.second);
    i.second->latch_.unlock();
    i.second->cv_.notify_all();
  }
  row_lock_map_latch_.unlock();
}

void LockManager::DFSFirstCycle(txn_id_t begin, txn_id_t now, bool &cycle_exist, std::vector<txn_id_t> &path) {
  if (cycle_exist) {
    return;
  }
  path.emplace_back(now);
  visit_[now] = true;
  if (waits_for_.find(now) == waits_for_.end()) {
    visit_[now] = false;
    path.pop_back();
    return;
  }
  for (const auto &i : waits_for_[now]) {
    if (i == begin) {
      cycle_exist = true;
      return;
    }
    if (visit_[i]) {
      continue;
    }
    DFSFirstCycle(begin, i, cycle_exist, path);
    if (cycle_exist) {
      return;
    }
  }
  if (cycle_exist) {
    return;
  }
  path.pop_back();
  visit_[now] = false;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_meta_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                plan_->table_oid_)) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw ExecutionException("");
    }
  }
  iter_ = std::make_shared<TableIterator>(table_meta_->table_->Begin(GetExecutorContext()->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if ((*iter_) == table_meta_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
      return false;
    }
    std::vector<std::pair<table_oid_t, RID>> v;
    std::vector<table_oid_t> v1;
    for (const auto &i : *exec_ctx_->GetTransaction()->GetSharedRowLockSet()) {
      for (const auto &j : i.second) {
        v.emplace_back(std::pair(i.first, j));
      }
    }
    for (const auto &i : *exec_ctx_->GetTransaction()->GetIntentionSharedTableLockSet()) {
      v1.emplace_back(i);
    }
    for (const auto &i : v) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), i.first, i.second);
    }
    for (const auto &i : v1) {
      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), i);
    }
    return false;
  }
  *tuple = Tuple(**iter_);
  *rid = RID((*iter_)->GetRid().GetPageId(), (*iter_)->GetRid().GetSlotNum());
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                              plan_->table_oid_, *rid)) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw ExecutionException("");
    }
  }
  ++(*iter_);
  return true;
}

}  // namespace bustub

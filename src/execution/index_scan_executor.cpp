//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_meta = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_meta->index_.get());
  table_meta_ = GetExecutorContext()->GetCatalog()->GetTable(index_meta->table_name_);
  if (table_meta_->table_->Begin(GetExecutorContext()->GetTransaction()) == table_meta_->table_->End()) {
    is_empty_ = true;
  } else {
    iter_ = tree_->GetBeginIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_empty_ || iter_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = iter_.operator*().second;
  ++iter_;
  table_meta_->table_->GetTuple(*rid, tuple, GetExecutorContext()->GetTransaction());
  return true;
}

}  // namespace bustub

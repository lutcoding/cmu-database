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
  iter_ = std::make_shared<TableIterator>(table_meta_->table_->Begin(GetExecutorContext()->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if ((*iter_) == table_meta_->table_->End()) {
    return false;
  }
  *tuple = Tuple(**iter_);
  *rid = RID((*iter_)->GetRid().GetPageId(), (*iter_)->GetRid().GetSlotNum());
  ++(*iter_);
  return true;
}

}  // namespace bustub

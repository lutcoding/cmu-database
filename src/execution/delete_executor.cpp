//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto table_meta = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  if (table_meta == nullptr) {
    return;
  }
  table_meta_ = table_meta;
  index_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_->name_);
  child_executor_->Init();
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                              plan_->table_oid_)) {
    exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
    throw ExecutionException("");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                              plan_->table_oid_, *rid)) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      RollBackIndex();
      throw ExecutionException("");
    }
    num_++;
    table_meta_->table_->MarkDelete(*rid, GetExecutorContext()->GetTransaction());
    if (!index_.empty()) {
      index_write_set_.emplace_back(std::pair(child_tuple, *rid));
      for (auto &i : index_) {
        i->index_->DeleteEntry(
            child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), i->key_schema_, i->index_->GetKeyAttrs()),
            *rid, GetExecutorContext()->GetTransaction());
      }
    }
  }
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(num_));
  *tuple = Tuple(values, &GetOutputSchema());
  flag_ = true;
  return true;
}

void DeleteExecutor::RollBackIndex() {
  if (index_.empty()) {
    return;
  }
  for (auto &i : index_) {
    for (const auto &j : index_write_set_) {
      Tuple child_tuple = j.first;
      i->index_->InsertEntry(
          child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), i->key_schema_, i->index_->GetKeyAttrs()),
          j.second, exec_ctx_->GetTransaction());
    }
  }
}

}  // namespace bustub

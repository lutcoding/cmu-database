//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_meta_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  index_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_->name_);
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    table_meta_->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    if (!index_.empty()) {
      for (auto &i : index_) {
        i->index_->InsertEntry(
            child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), i->key_schema_, i->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
      }
    }
    num_++;
  }
  std::vector<Value> vals;
  vals.emplace_back(ValueFactory::GetIntegerValue(num_));
  *tuple = Tuple{vals, &GetOutputSchema()};
  flag_ = true;
  return true;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_);
  tree_index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    Value v;
    v = plan_->key_predicate_->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    std::vector<RID> rids;
    tree_index_->index_->ScanKey(Tuple(std::vector<Value>{v}, &tree_index_->key_schema_), &rids,
                                 exec_ctx_->GetTransaction());
    if (rids.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->inner_table_schema_->GetColumnCount(); ++i) {
          values.emplace_back(ValueFactory::GetNullValueByType(plan_->inner_table_schema_->GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
    } else {
      Tuple t{};
      table_meta_->table_->GetTuple(rids[0], &t, exec_ctx_->GetTransaction());
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->inner_table_schema_->GetColumnCount(); ++i) {
        values.emplace_back(t.GetValue(&*plan_->inner_table_schema_, i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub

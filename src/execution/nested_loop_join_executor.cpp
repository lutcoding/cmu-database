//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple child_tuple{};
  RID rid{};
  while (left_executor_->Next(&child_tuple, &rid)) {
    left_tuples_.emplace_back(child_tuple);
  }
  while (right_executor_->Next(&child_tuple, &rid)) {
    right_tuples_.emplace_back(child_tuple);
  }
  left_iter_ = left_tuples_.begin();
  right_iter_ = right_tuples_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_iter_ != left_tuples_.end()) {
    while (right_iter_ != right_tuples_.end()) {
      auto v = plan_->predicate_->EvaluateJoin(&*left_iter_, left_executor_->GetOutputSchema(), &*right_iter_,
                                               right_executor_->GetOutputSchema());
      if (v.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
        flag_ = true;
        std::vector<Value> values;
        values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                       right_executor_->GetOutputSchema().GetColumnCount());
        for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); ++j) {
          values.emplace_back(left_iter_->GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); ++j) {
          values.emplace_back(right_iter_->GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        if (++right_iter_ == right_tuples_.end()) {
          ++left_iter_;
          right_iter_ = right_tuples_.begin();
          flag_ = false;
        }
        return true;
      }
      ++right_iter_;
    }
    if (plan_->GetJoinType() == JoinType::LEFT && !flag_) {
      std::vector<Value> values;
      values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                     right_executor_->GetOutputSchema().GetColumnCount());
      for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); ++j) {
        values.emplace_back(left_iter_->GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); ++j) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      right_iter_ = right_tuples_.begin();
      ++left_iter_;
      return true;
    }
    right_iter_ = right_tuples_.begin();
    ++left_iter_;
  }

  return false;
}

}  // namespace bustub

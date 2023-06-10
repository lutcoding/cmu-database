#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID rid{};
  while (child_executor_->Next(&child_tuple, &rid)) {
    values_.emplace_back(child_tuple);
  }
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys = plan_->GetOrderBy();
  Schema s = child_executor_->GetOutputSchema();
  std::sort(values_.begin(), values_.end(), [order_bys, s](const Tuple &l, const Tuple &r) -> bool {
    for (const auto &pair : order_bys) {
      Value left;
      Value right;
      left = pair.second->Evaluate(&l, s);
      right = pair.second->Evaluate(&r, s);
      if (left.CompareEquals(right) == CmpBool::CmpTrue) {
        continue;
      }
      if (pair.first == OrderByType::DEFAULT || pair.first == OrderByType::ASC) {
        return left.CompareLessThan(right) == CmpBool::CmpTrue;
      }
      return left.CompareGreaterThan(right) == CmpBool::CmpTrue;
    }
    return true;
  });
  iter_ = values_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == values_.end()) {
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
}

}  // namespace bustub

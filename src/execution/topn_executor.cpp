#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      s_(child_executor_->GetOutputSchema()),
      order_(plan_->order_bys_) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID rid{};
  while (child_executor_->Next(&child_tuple, &rid)) {
    queue_.push(TupleCompare(child_tuple, &s_, &order_));
    if (queue_.size() > plan_->n_) {
      queue_.pop();
    }
  }
  while (!queue_.empty()) {
    stack_.push(queue_.top().GetTuple());
    queue_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!stack_.empty()) {
    *tuple = stack_.top();
    stack_.pop();
    return true;
  }
  return false;
}

}  // namespace bustub

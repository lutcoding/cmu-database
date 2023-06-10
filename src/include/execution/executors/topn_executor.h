//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <stack>
#include <utility>
#include <vector>
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  class TupleCompare {
   public:
    TupleCompare(const Tuple &t, Schema *s, std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order)
        : t_(t), s_(s), order_(order) {}
    auto GetTuple() const -> Tuple { return t_; }
    auto operator<(const TupleCompare &r) const -> bool {
      for (const auto &pair : *order_) {
        Value left;
        Value right;
        left = pair.second->Evaluate(&t_, *s_);
        right = pair.second->Evaluate(&r.t_, *s_);
        if (left.CompareEquals(right) == CmpBool::CmpTrue) {
          continue;
        }
        if (pair.first == OrderByType::DEFAULT || pair.first == OrderByType::ASC) {
          return left.CompareLessThan(right) == CmpBool::CmpTrue;
        }
        return left.CompareGreaterThan(right) == CmpBool::CmpTrue;
      }
      return true;
    }

   private:
    Tuple t_;
    Schema *s_;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_;
  };

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  Schema s_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_;
  std::priority_queue<TupleCompare, std::vector<TupleCompare>> queue_;
  std::stack<Tuple> stack_;
};

}  // namespace bustub

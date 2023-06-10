#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &child_plan = limit.GetChildPlan();
    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort = dynamic_cast<const SortPlanNode &>(*child_plan);
      const auto &order_bys = sort.GetOrderBy();
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort.GetChildPlan(), order_bys,
                                            limit.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  single_key_ = AggregateKey{std::vector<Value>{ValueFactory::GetIntegerValue(0)}};
  if (plan_->group_bys_.empty()) {
    no_group_bys_ = true;
  }
  Tuple child_tuple{};
  RID rid{};
  while (child_->Next(&child_tuple, &rid)) {
    AggregateKey key;
    AggregateValue value;
    if (no_group_bys_) {
      key = single_key_;
    } else {
      key = MakeAggregateKey(&child_tuple);
    }
    value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
    is_empty_ = false;
  }
  if (is_empty_ && no_group_bys_) {
    std::vector<Value> values;
    for (const auto &e : plan_->agg_types_) {
      if (e == AggregationType::CountStarAggregate) {
        values.emplace_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }
    aht_.InsertCombine(single_key_, AggregateValue{values});
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!flag_) {
    aht_iterator_ = aht_.Begin();
    flag_ = true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> v;
  if (!no_group_bys_) {
    if (is_empty_) {
      return false;
    }
    v = aht_iterator_.Key().group_bys_;
  }
  v.insert(v.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{v, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

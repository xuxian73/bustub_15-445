//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      pos_(-1) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    Value left_key = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    std::vector<Value> values;
    for (size_t i = 0; i < left_child_->GetOutputSchema()->GetColumnCount(); i++) {
      values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
    }
    HashJoinKey key{left_key};
    if (left_ht_.find(key) == left_ht_.end()) {
      left_ht_.insert({key, {values}});
    } else {
      left_ht_[key].push_back(values);
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (pos_ == -1) {
      if (!right_child_->Next(&right_tuple_, &right_rid_)) {
        return false;
      }
    }
    auto right_key = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, plan_->GetRightPlan()->OutputSchema());
    auto iter = left_ht_.find(HashJoinKey{right_key});
    if (iter == left_ht_.end()) {
      pos_ = -1;
    } else {
      ++pos_;
      for (; pos_ < int32_t(iter->second.size()); pos_++) {
        if (iter->first == HashJoinKey{right_key}) {
          std::vector<Value> values;
          for (auto &col : plan_->OutputSchema()->GetColumns()) {
            auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
            if (expr->GetTupleIdx() == 0) {
              values.push_back(iter->second[pos_][expr->GetColIdx()]);
            } else {
              values.push_back(right_tuple_.GetValue(plan_->GetRightPlan()->OutputSchema(), expr->GetColIdx()));
            }
          }
          *tuple = Tuple(values, plan_->OutputSchema());
          *rid = tuple->GetRid();
          return true;
        }
      }
      pos_ = -1;
    }
  }
}

}  // namespace bustub

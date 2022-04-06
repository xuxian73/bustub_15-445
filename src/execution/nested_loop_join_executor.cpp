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
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_valid_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!left_valid_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    while (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      right_executor_->Init();
    }
    if (plan_->Predicate() == nullptr || plan_->Predicate()
                                             ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                            &right_tuple, plan_->GetRightPlan()->OutputSchema())
                                             .GetAs<bool>()) {
      std::vector<Value> values;
      auto output_schema = GetOutputSchema();
      values.reserve(output_schema->GetColumns().size());
      for (auto &col : output_schema->GetColumns()) {
        auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
        if (expr->GetTupleIdx() == 0) {
          values.push_back(left_tuple_.GetValue(plan_->GetLeftPlan()->OutputSchema(), expr->GetColIdx()));
        } else {
          values.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), expr->GetColIdx()));
        }
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID{}, nullptr), end_(nullptr, RID{}, nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (cur_ != end_) {
    auto predicate = plan_->GetPredicate();
    if (predicate == nullptr) {
      goto ok;
    }
    auto value = predicate->Evaluate(&(*cur_), &table_info_->schema_);
    if (value.GetAs<bool>()) {
      goto ok;
    }
    ++cur_;
  }
  return false;

ok:
  std::vector<Value> values;
  auto output_schema = GetOutputSchema();
  for (auto &col : output_schema->GetColumns()) {
    auto value = cur_->GetValue(&table_info_->schema_, table_info_->schema_.GetColIdx(col.GetName()));
    values.push_back(value);
  }
  *tuple = Tuple(values, output_schema);
  *rid = cur_->GetRid();
  ++cur_;
  return true;
}

}  // namespace bustub

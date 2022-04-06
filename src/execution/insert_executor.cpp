//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), raw_insert_index_(0), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (child_executor_ && !plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (raw_insert_index_ == plan_->RawValues().size()) {
      return false;
    }
    *tuple = Tuple(plan_->RawValues()[raw_insert_index_], &table_info_->schema_);
    raw_insert_index_++;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  }
  if (!table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (auto &index_info : index_infos_) {
    const auto index_key =
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub

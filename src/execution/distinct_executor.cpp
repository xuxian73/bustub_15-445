//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    DistinctKey key;
    for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); ++i) {
      key.keys.push_back(tuple->GetValue(plan_->OutputSchema(), i));
    }
    hash_t hash_value = std::hash<DistinctKey>()(key);
    if (produced_.find(hash_value) == produced_.end()) {
      produced_.insert({hash_value, {key}});
      return true;
    } else {
      bool isExist = false;
      for (auto &exist_key : produced_[hash_value]) {
        if (exist_key == key) {
          isExist = true;
          break;
        }
      }
      if (!isExist) {
        produced_[hash_value].push_back(key);
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub

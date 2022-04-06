//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"
#include "common/util/hash_util.h"

namespace bustub {

/** The hash of a tuple. */
struct DistinctKey {
  std::vector<Value> keys;

  bool operator==(const DistinctKey &other) const {
    if (keys.size() != other.keys.size()) {
      return false;
    }
    for (size_t i = 0; i < keys.size(); i++) {
      if (keys[i].CompareEquals(other.keys[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The hash of tuple already produced */
  std::unordered_map<hash_t, std::vector<DistinctKey>> produced_;
};
}  // namespace bustub

namespace std {
  template <>
  struct hash<bustub::DistinctKey> {
    size_t operator()(const bustub::DistinctKey &hash) const {
      bustub::hash_t hash_val = 0;
      for (const auto &key : hash.keys) {
        bustub::HashUtil::CombineHashes(bustub::HashUtil::Hash(&key), hash_val);
      }
      return hash_val;
    }
  };  
} // namespace std


//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** HashKey represents a key in the hash table. */
struct HashJoinKey {
  /** The key. */
  Value key_;

  /** The constructor. */
  HashJoinKey() = default;
  explicit HashJoinKey(const Value &k) : key_(k) {}

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const HashJoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};
}  // namespace bustub

namespace std {
/**
 * A hash function for HashJoinKey.
 * @param key the key to be hashed
 * @return the hash value of the key
 */
template <>
struct hash<bustub::HashJoinKey> {
  size_t operator()(const bustub::HashJoinKey &key) const { return bustub::HashUtil::HashValue(&key.key_); }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor that produces tuples for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_child_;
  /** The right child executor that produces tuples for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_child_;
  /** The hash table for the left child. */
  std::unordered_map<HashJoinKey, std::vector<std::vector<Value>>> left_ht_;
  /** The current position in the left child hash table bucket. */
  int32_t pos_;
  /** The current tuple of right child. */
  Tuple right_tuple_;
  /** The current RID of right child. */
  RID right_rid_;
};

}  // namespace bustub

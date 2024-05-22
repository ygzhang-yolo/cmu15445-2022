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
#include "common/exception.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

/*
 * Init: 获取TableIterator的首个tuple
 */
void SeqScanExecutor::Init() {
  /*
   * 事务相关
   * 1. 读未提交不需要上任何锁, 默认就是这种级别;
   * 2. 其他隔离级别需要上一个表的意向共享锁; 表示事务要在某个表(或行)上获取共享读锁;
   */
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}
/*
 * Next: 遍历返回每一个匹配谓词的tuple和他的原始RID;
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    // 遍历到end, 没找到return false;
    if (table_iter_ == table_info_->table_->End()) {
      /*
       * 事务相关
       * 1. 只有读未提交需要释放掉之前的锁, 因为可重复读需要建立快照
       * 2. 释放锁包括所有行锁和当前的表锁
       */
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
        table_oid_t oid = table_info_->oid_;
        for (auto rids : locked_row_set) {
          exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rids);
        }
        exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
      }
      return false;
    }
    *tuple = *table_iter_;
    *rid = tuple->GetRid();
    ++table_iter_;
    // match的条件是, 谓词符合schema_
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());
  /*
   * 事务相关
   * 1. 对访问的所有行, 先拿一个共享锁(读锁)
   */
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }

  return true;
}

}  // namespace bustub

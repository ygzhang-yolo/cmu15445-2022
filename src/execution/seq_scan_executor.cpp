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

/*
 * 构造函数，赋初值
 * */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

/*
 * 获取TableIterator的首个Tuple
 * */
void SeqScanExecutor::Init() {
  /*
   * exec_ctx_->GetTransaction()->GetIsolationLevel()
   * 执行程序在其中运行的执行程序上下文 -> 获得当前正在运行的事务 -> 获得事务的隔离级别
   * 1. 如果是最低级别 READ_UNCOMMITTED，不需要上锁
   * 2. 其他级别都需要上锁
   * */
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      /*
       * LockTable 给一个给定的事务在指定的表上加上特定锁，这个锁也要符合事务隔离级别
       * 意向共享锁（INTENTION_SHARED）：表级锁的辅助锁，表示事务要在某个表或页级锁上获取共享锁(读锁)。
       * 如果在等待过程中事务被中止，返回false
       * */
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  // 让table_iter从当前事务开始
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

/*
 * 对于行级锁存在：记录锁、间隙锁与Next-Key锁
 * 记录锁(record lock):
 *   对索引记录的锁，注意，它是针对索引记录，即它只锁定记录这一行数据;查询的是索引，则上的是记录锁，否则是普通的读/写锁(记录锁也是读/写锁的一种)
 * 间隙锁(Gap Lock):
 *   用于解决幻读问题，只在repeated_read隔离级别下有效;比如对要插入前后的记录也上锁
 * Next-Key 锁(临间锁):
 *   等价与间隙锁+记录锁，repeated_read下与间隙锁一起用于解决幻读问题
 *   解释：https://cloud.tencent.com/developer/article/1990145
 *      (mysql默认repeated_read)在mysql上相当于只有Next_Key锁，只是这个锁在不同索引情况下，可能是记录锁/间隙锁。
 * */

/*
 * 查询的Next-顺序扫描执行器(SeqScanExecutor)：说白了就是跳到下一个符合条件的行
 * 用于逐行遍历表并应用过滤谓词(Where等条件)，负责处理行的访问和锁管理，以适应不同的事务隔离级别。
 * 1. 向后遍历，直到找到符合条件的行
 *  1.1 如果遍历到了表的结尾，表明要退出了(要注意释放锁，如果有！
 *    -> 如果当前执行的上下文的隔离级别是read_committed(要等一个事务执行完，才可以读)，需要释放该事务所持有的所有行锁和表锁
 *      （如果是read_uncommitted，没有锁，不用管；而repeated_read是有额外的锁，要求在一个食物内读到的数据是一样的，也就是除了提交的）
 *  1.2 不满足条件，获取下一行
 * 2. 找到了对应的行，则代表可能要对当前行进行读取，所以要对非read_uncommitted隔离级别的事务获取读锁(这是为了保证在读取行数据时，该行不会被其他事务修改)
 *    获取失败则抛出异常
 *    获取成功就锁定了这个行了
 * */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iter_ == table_info_->table_->End()) {
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
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());

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

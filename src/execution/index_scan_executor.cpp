//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
/*
 * 将计划节点中的配置和数据库的目录信息结合起来，准备好执行索引扫描。通过这种方式，它能够有效地利用索引加速查询过程。
 * index_scan_executor:使用表的索引来快速定位数据。索引扫描一般用于执行基于索引的查询，如基于特定条件的查找或排序操作。
 * 而scan_executor适用于通过遍历整个表来查找数据，不依赖索引。
 * */

/*
 * 执行索引扫描操作的构造函数：
 * exec_ctx：提供了执行器执行操作时需要的所有运行时环境和依赖，包括访问事务管理、缓冲池管理、目录服务等
 * plan_: 指向索引扫描计划节点的指针
 * index_info_: 索引信息(GetCatalog->GetIndex获取)
 * table_info_: 表信息(GetCatalog->GetTable获取)
 * iter_：迭代器，用于遍历满足索引条件的表条目，要初始化(如果计划节点中定义了过滤谓词，则初始化一个条件过滤的迭代器，否则使用索引树的开始迭代器)
 * */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      iter_{plan_->filter_predicate_ != nullptr ? BPlusTreeIndexIteratorForOneIntegerColumn(nullptr, nullptr)
                                                : tree_->GetBeginIterator()} {}


/*
 * 如果过滤谓词为空，代表全表扫描，直接返回
 * 过滤谓词不为空，代表有筛选条件：
 *  1. 先尝试获取读锁(如果是read_uncommitted隔离级别的事务，则无锁，直接到2)，获取锁失败就抛异常
 *  2. 成功获取读锁后，从过滤谓词中提取出用于索引查找的常量值(其他地方实现)，并将这个值用于初始化索引查找
 *  3. tree_->ScanKey 方法使用这个值来检索所有匹配的行标识符（RID），这些RID存储在 rids_ 容器中
 *  4. 初始化 rid_iter_，这是一个迭代器，用于遍历 rids_ 容器中的所有RID
 * */
void IndexScanExecutor::Init() {
  if (plan_->filter_predicate_ != nullptr) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
        if (!is_locked) {
          throw ExecutionException("IndexScan Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("IndexScan Executor Get Table Lock Failed" + e.GetInfo());
      }
    }
    const auto *right_expr =
        dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    // 3. 检索所有匹配的行标识符(RID)，这些RID存储在rids_容器中
    tree_->ScanKey(Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, exec_ctx_->GetTransaction());
    rid_iter_ = rids_.begin();
  }
}


/*
 * 如果谓词不存在，就是直接遍历，未到末端就iter_++即可：
 *  1. 如果iter_已经到tree_索引树的末端，则直接返回false
 *  2. 未到末端，从迭代器中获取当前的RID，以便后面用RID检索到对应元组
 *  3. iter_++ , 并返回检索到的元组
 * 谓词存在时：
 *  1. 检查rid_iter_是否已经遍历完rids_(所有符合条件的RID) ，若完，则返回false(代表并未向下Next)
 *  2. 未遍历完，步骤同从前，获取这个事务的隔离级别，在非read_uncommitted时，尝试获取(即上)读锁，如获取失败则抛出异常
 *    上完读锁后，就可以读了，使用从索引中获得的RID中定位到相应元组，存储与tuple中
 *    更新迭代器指向下一个元素，并返回结果
 * */
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->filter_predicate_ != nullptr) {
    if (rid_iter_ != rids_.end()) {
      *rid = *rid_iter_;
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                                LockManager::LockMode::SHARED, table_info_->oid_, *rid);
          if (!is_locked) {
            throw ExecutionException("IndexScan Executor Get Table Lock Failed");
          }
        } catch (TransactionAbortException e) {
          throw ExecutionException("IndexScan Executor Get Row Lock Failed");
        }
      }

      auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
      rid_iter_++;
      return result;
    }
    return false;
  }
  if (iter_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (*iter_).second;
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;

  return result;
}

}  // namespace bustub

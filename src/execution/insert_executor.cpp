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

/*
 * 构造函数
 * exec_ctx：执行上下文，提供执行器执行操作期间所需的所有环境和依赖
 *  -> 父类初始化 AbstractExecutor(exec_ctx): 用exec_ctx实例化一个AbstractExecutor对象
 * plan: 指向插入计划节点(InsertPlanNode)的指针
 * child_executor: 指向子执行器的唯一指针(unique_ptr)；用来获取插入操作所需的数据
 *
 * this->table_info_：维护一个表的元数据
 * 这里plan_->table_oid_是要插入的表的id
 * */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}


/*
 * 在插入前，要先上写锁
 * 1. 初始化child_executor(自上向下)
 * 2. 尝试给当前事务添加表级写锁：
 *    添加失败则报错；
 *    添加成功后，调用GetTableIndexes函数：返回给定表上每个索引的IndexInfo*向量，如果表存在但没有为其创建索引，则为空向量
 * */
void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}


/*
 * 插入的Next:
 *  1. 结束检查：is_end_为true，代表所有需要插入的元组已经处理完毕，返回false(代表未插入就已经结束返回)
 *  2. 插入：
 *    2.1 从子执行器(child_executor)中循环获取要插入的元组(to_insert_tuple)和相关的行标识符(emit_rid)
 *    2.2 调用InsertTuple方法插入
 *      插入成功，则为新插入的行获取写锁(EXCLUSIVE)，如果锁定失败就抛出异常；
 *      获取写锁后，要更新相关索引(如果表有索引，对于每个索引，使用元组中的键和行标识符更新索引；涉及从元组中提取出适合索引的键，然后在索引上插入新的键/行标识符对)
 *  3. 完成全部插入返回：
 *    记录成功插入的元组数量(insert_count)，作为tuple放入*tuple中
 *    is_end_设置为true，返回true(代表插入完后返回)
 * */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_insert_tuple{};
  RID emit_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());

    if (inserted) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!is_locked) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
      // 更新索引 从元组中提取出适合索引的键，然后在索引上插入新的键/行标识符对。
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      insert_count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, insert_count);
  // 元组
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub

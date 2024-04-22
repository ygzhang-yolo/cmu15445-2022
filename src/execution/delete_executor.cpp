//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

/*
 * delete_executor: 构造器，同insert_executor
 * plan_: 指向要删除位置的指针
 * */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

/*
 * 同insert_executor
 * 自顶向上Init，先初始化child_executor
 * 然后上表锁：delete与insert都是对表上写锁，即表级排他锁
 * 上锁失败就抛出异常；上锁成功则将调用GetTableIndexes函数：返回给定表上每个索引的IndexInfo*向量，如果表存在但没有为其创建索引，则为空向量
 * (翻译一下：获得表上的每个索引，这样才能有后面的遍历或查找)
 * */
void DeleteExecutor::Init() {
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}


/*
 * 同insert_executor
 * 1. 结束检查：is_end_为true，代表所有需要删除的元组已经处理完毕，返回false(代表未删除就已经结束返回)
 * 2. 删除：
 *  2.1 从子执行器(child_executor)中循环获取要删除的元组(to_delete_tuple)和相关的行标识符(emit_rid)
 *  2.2 要删除，在删前要对该行上写锁(上锁失败则抛出异常)
 *    上锁成功，调用table_->MarkDelete函数删除
 *    若删除成功，则更新索引(这部分同insert_executor)
 * 3. 完成全部删除返回：
 *  记录成功删除的元组数量(delete_count)，作为tuple放入*tuple中
 *  is_end_设置为true，返回true(代表插入完后返回)
 * */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;

  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, emit_rid);
      if (!is_locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }

    bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

    if (deleted) {
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_delete_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->DeleteEntry(to_delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      delete_count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub

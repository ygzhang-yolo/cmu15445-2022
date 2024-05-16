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

#include <algorithm>
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() {
  // child和table对应的index
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // is end, return false, 幂等性去重
  if (is_end_) {
    return false;
  }
  Tuple to_delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;
  // 遍历table的所有index
  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    // MarkDelete: 将tuple标记为delete; (删除不是直接删除, 而是标记为不存在)
    bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());
    if (deleted) {
      // DeleteEntry, 将table所有的index涉及的key都删了
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_delete_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->DeleteEntry(to_delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      delete_count++;
    }
  }
  // 返回一个tuple, 包含了一个integer表示table中有多少行受到了影响
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub

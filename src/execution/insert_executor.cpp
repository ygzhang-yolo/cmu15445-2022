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
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
    // init child executor
    child_executor_->Init();
    // init table index
    table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    // is_end = return, 感觉像一个幂等去重的
    if(is_end_) {
        return false;
    }

    Tuple to_insert_tuple{};
    RID emit_rid;
    int32_t insert_count = 0;
    // 
    while(child_executor_->Next(&to_insert_tuple, &emit_rid)) {
        // InsertTuple: 将tuple插入到对应的page中
        bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());
        if (inserted) {
          // 插入成功则更新索引, InsertEntry将新key插入到table的所有索引中
          std::for_each(
              table_indexes_.begin(), table_indexes_.end(),
              [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                index->index_->InsertEntry(
                    to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
                    *rid, exec_ctx->GetTransaction());
              });
          insert_count++;
        }
    }
    // 返回一个tuple, 包含了一个integer表示table中有多少行受到了影响
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(TypeId::INTEGER, insert_count);
    *tuple = Tuple{values, &GetOutputSchema()};
    is_end_ = true;
    return true;
}

}  // namespace bustub

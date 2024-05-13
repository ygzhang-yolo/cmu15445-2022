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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

/*
 * Init: 获取TableIterator的首个tuple
*/
void SeqScanExecutor::Init() {
  // 让table_iter从当前事务开始
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}
/*
 * Next: 遍历返回每一个匹配的tuple和他的原始RID;
*/
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iter_ == table_info_->table_->End()) {
      return false;
    }
    *tuple = *table_iter_;
    *rid = tuple->GetRid();
    ++table_iter_;
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());
  return true;
}

}  // namespace bustub

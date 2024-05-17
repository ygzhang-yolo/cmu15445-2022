//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
/*
 * AbstractExecutor(exec_ctx)、plan_{plan} 同之前 略
 * child_: 指向外部表(左表)数据的指针
 */
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_(std::move(child_executor)),
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_->Init(); }
/*
 * Next是连接器生成连接后的结果的方法(使用索引加速链接操作)
 * SELECT * FROM Employees e xx JOIN Departments d ON e.DepartmentID = d.ID
 * 1. while遍历左表中的每一条数据
 *   计算左表中用于连接右表时 用于比较的列表名键值(这里是DepartmentID)
 *   用这些列表名在右表中查询，得到对应的列的RID
 *   如果没找到，则判断是不是Left连接，是则将右表中该行所有字段赋为null连接，不是(inner)则不要该行
 *   如果找到了，则不区分left 与 inner连接：
 *    用RID取出右表对应行
 *    连接左右表数据：
 *      将其元组值分别添加到vals向量中，并将其包装成tuple
 * 
*/
auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 临时变量
  Tuple left_tuple{};
  RID emit_rid{};
  std::vector<Value> vals;
  // range child's Next, 遍历左表中的所有数据
  while(child_->Next(&left_tuple, &emit_rid)) {
    Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_->GetOutputSchema());
    std::vector<RID> rids;
    tree_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    Tuple right_tuple{};
    if(!rids.empty()) {
      // 用rid获得右表对应元组的数据
      table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
      for (uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
      }
      // 从获得的右表元组行中，获得每个字段的数据，并写入
      for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), idx));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
    if(plan_->GetJoinType() == JoinType::LEFT) {
      for(uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
      }
      // 按照右边的每个字段信息, 分别赋为对应的null类型并写入
      for(uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
  }
  return false;
}
// auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   Tuple left_tuple{};
//   RID emit_rid{};
//   std::vector<Value> vals;
//   while (child_->Next(&left_tuple, &emit_rid)) {
//     Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_->GetOutputSchema());
//     std::vector<RID> rids;
//     tree_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

//     Tuple right_tuple{};
//     if (!rids.empty()) {
//       // 用rid获得右表对应元组的数据
//       table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
//       for (uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
//         vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
//       }
//       // 从获得的右表元组行中，获得每个字段的数据，并写入
//       for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
//         vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), idx));
//       }
//       *tuple = Tuple(vals, &GetOutputSchema());
//       return true;
//     }
//     if (plan_->GetJoinType() == JoinType::LEFT) {
//       for (uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
//         vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
//       }
//       // 按照右表的每个字段信息，分别赋为对应的null类型并写入
//       for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
//         vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
//       }
//       *tuple = Tuple(vals, &GetOutputSchema());
//       return true;
//     }
//   }
//   return false;
// }

}  // namespace bustub

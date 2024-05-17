//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/*
 * 构造函数初始化
 * 父类初始化；
 * plan_: 指向聚合计划节点的指针，其中定义了聚合操作(聚合操作的值、类型、所针对的列、是否分组等等)
 * aht_: 执行聚合操作时存储和管理中间聚合结果、
 * aht_iterator: aht_的迭代器
 * */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

/*
 * Init: 在init中把查询到的结果都聚合起来
*/
void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  // 遍历child->Next获取每个符合条件的记录, InsertCombine 结果插入到聚合表中
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  // 如果是空的先初始化
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertIntialCombine();
  }
  // 初始化聚合结果aht的迭代器
  aht_iterator_ = aht_.Begin();
}

/*
 * 这个函数就是把对应的聚合结果取出来
 * 例：SELECT COUNT(*), MAX(age), SUM(salary) FROM employees
 *    这里有三个聚合函数，aht_中就有三个结果值
 *    调用一次Next函数就是获得一个结果值
 * */
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;

  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

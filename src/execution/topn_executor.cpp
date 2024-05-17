#include "execution/executors/topn_executor.h"
#include "binder/bound_order_by.h"

namespace bustub {

/* 构造函数 */
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

/* Init: */
void TopNExecutor::Init() {
  // child next
  child_->Init();
  // cmp函数用来比较order keys
  auto cmp = [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
    for (const auto &order_key : order_bys) {
      switch (order_key.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
      }
    }
    return false;
  };
  // priority queue 的堆来实现top N, 选择出top N的tuple
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple child_tuple{};
  RID child_rid;
  while(child_->Next(&child_tuple, &child_rid)) {
    pq.push(child_tuple);
    if(pq.size() > plan_->GetN()) {
        pq.pop();
    }
  }
  while(!pq.empty()) {
    child_tuples_.push(pq.top());
    pq.pop();
  }
}
/* Next: */
auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(child_tuples_.empty()) {
        return false;
    }
    *tuple = child_tuples_.top();
    *rid = tuple->GetRid();
    child_tuples_.pop();
    return true;
}

}  // namespace bustub

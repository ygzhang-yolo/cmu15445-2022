//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
// ====================== Task1: Lock Manager ==========================//
/*
* LockTable: 获取表级锁
*/
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1. 检查事务的隔离级别和状态
  // 1.1 读未提交
  // 1.1.1 不允许申请共享锁或意向共享锁
  // 1.1.2 如果事务状态是shrinking, 不允许申请排他锁或意向排他锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  // 1.2 读提交
  // 如果事务状态是shrinking, 只允许申请意向共享锁和共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  // 1.3 可重复读
  // 如果事务状态是shrinking, 不允许申请任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  // 2. 查找和创建锁请求队列
  // 上表锁 table_lock, 需要通过map, 根据oid找到对应的表锁
  table_lock_map_latch_.lock();  //访问table_lock_map上的锁
  // 如果不存在oid对应的表锁, 则创建一个, 并存入map
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();  // request lock queue
  table_lock_map_latch_.unlock();
  // 3. 处理锁的升级
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {  // 找得到当前事务的锁请求, 说明要升级锁而非重新加锁
      // 3.1 如果锁模式相同, 无需升级, 直接return
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      // 3.2 如果已经有其他事务正在进行锁升级, 终止事务并抛出异常
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // 3.3 检查锁升级的兼容性, 仅允许某些特定的锁模式转换, 否则中止事务并抛出异常
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 3.4 删除旧的锁请求, 从请求队列中删除当前事务的旧锁请求, 更新锁集合
      // 3.4.1 从请求队列中删除旧的锁请求，并更新锁集合;
      lock_request_queue->request_queue_.remove(request);
      this->InsertOrDeleteTableLockSet(txn, request, false);

      // 3.4.2 创建新的锁请求，并插入到锁请求队列中;
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid); //新的锁请求

      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request); //更新锁的集合
      // 3.4.3 标记当前事务正在进行锁升级;
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      // 3.5 尝试授予升级后的锁
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {  // GrantLock确保新锁是符合规定
        // NOTE: 利用条件变量等待锁的授予, 如果事务在等待过程中被中止, 移除锁请求并通知所有等待的事务
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      // 3.6 锁升级成功后的处理
      lock_request_queue->upgrading_ = INVALID_TXN_ID;  // 释放锁升级标记
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);  //更新事务的锁集合

      if (lock_mode != LockMode::EXCLUSIVE) { // 只要不是排他锁, 都需要通知所有等待的事务
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  // 4. 添加新的锁请求(如果没有锁升级, 则添加新的锁请求到队列中)
  // 4.1 创建新的锁请求并插入锁请求队列末尾
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  // 4.2 尝试授予新的锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // 4.3 新锁请求成功后的处理
  lock_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

/*
* UnlockTable: 释放表级锁
*/
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock(); // 获取table lock map的锁

  // 1. 检查表锁是否存在, 如果表锁不存在, 解锁并中止事务抛出异常
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 2. 检查行锁
  // 2.1 获取事务持有的共享行锁和排他行锁
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  // 2.2 检查事务是否在表上仍持有任何行锁, 如果事务在表上持有行锁, 则解锁并中止事务抛出异常
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 3. 获取锁请求队列
  auto lock_request_queue = table_lock_map_[oid]; //表对应的锁请求队列

  lock_request_queue->latch_.lock();  // 锁定请求队列的互斥锁
  table_lock_map_latch_.unlock();

  // 4. 遍历锁请求队列
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    // 4.1 找到当前事务持有且已授予的锁请求
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // 4.2 从请求队列中移除该锁请求，通知所有等待锁的事务, 并解锁lock_request_queue_latch
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // 4.3 更新事务状态, 根据事务的隔离级别和锁模式, 如果事务处于COMMITED或ABORTED则更新为SHRINKING
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      // 4.4 更新锁集合
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      return true;
    }
  }
  // 5. 如果找不到对应的锁, 解锁请求队列的互斥锁，并中止事务抛出异常
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

/* 行锁: 获取记录rid上的行锁 
* LockRow: 获取记录rid上的行锁
*/
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1. 检查不允许的锁类型(行锁不允许获取表级意向锁: 意向共享、意向排他、共享意向排他锁), 中止事务并抛出异常
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 2. 检查事务的隔离级别和状态
  // 2.1 读未提交
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // 不允许请求共享锁或意向锁
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    // 如果事务状态是shrinking, 不允许请求排他锁或意向排他锁
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 2.2 读已提交, 如果事务状态是shrinking, 只允许请求意向共享锁和共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 2.3 可重复读, 如果事务状态是shrinking, 不允许请求任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 3. 检查表级锁, 如果请求排他锁, 需要确保事务在表级已经持有适当的锁(排他锁、意向排他锁或共享意向排他锁), 否则中止事务并抛出异常
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // 4. 查找和创建锁请求队列
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // 5. 处理锁升级操作, 内容基本同表锁的锁升级过程
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteRowLockSet(txn, upgrade_lock_request, true);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // 6. 添加新的锁请求
  // 6.1 创建新的锁请求并插入请求队列末尾
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  // 6.2 尝试授予新的锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // 6.3 新锁请求成功后的处理
  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

/*
* UnlockRow: 解锁记录rid上的行锁
*/
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();   // 获取row lock map的锁
  // 1. 检查行锁是否存在, 如果行锁不存在, 解锁并中止事务, 抛出异常
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 2. 获取锁请求队列
  auto lock_request_queue = row_lock_map_[rid]; //行对应的锁请求队列

  lock_request_queue->latch_.lock();  // 锁定请求队列的互斥锁
  row_lock_map_latch_.unlock();

  // 3. 遍历锁请求队列
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
      // 3.1 找到当前事务并确保持有已授予的锁请求
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // 3.2 从锁队列中移除该锁请求, 并通知所有等待锁的事务, 并解锁lock_request_queue_latch
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // 3.3 更新事务状态, 根据事务的隔离级别和锁模式, 如果事务处于commited或者aborted则更新为shrinking
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      // 3.4 更新锁集合
      InsertOrDeleteRowLockSet(txn, lock_request, false);
      return true;
    }
  }

  // 4. 如果找不到对应的锁, 解锁请求队列的互斥锁, 并中止事务抛出异常
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

//======================= Task2: Deadlock Detection ====================// 
/*
* AddEdge: 在图中添加一条从t1到t2的边, 如果已存在不需要执行任何操作
*/
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  this->txn_set_.insert(t1);
  this->txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}
/*
* RemoveEdge: 从图中删除t1到t2的边, 如果不存在则不需要执行任何操作
*/
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // 1. 在事务t1的等待图waits_for_中查找t1->t2的边
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  // 2. 如果边存在, 从图中删除; 不存在则不需要执行任何操作
  if(iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}
/*
* HasCycle: DFS检查是否有环, 如果找得到环将最新的TX ID存储在txn_id中返回true;
*/
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 遍历所有事务集合, 以所有事务作起点进行Dfs
  for (auto const &start_txn_id : txn_set_) {
    if (Dfs(start_txn_id)) {
      // 如果有环, 则需要找出环的路径上, txn_id最大的事务返回
      *txn_id = *active_set_.begin();
      for (auto const &active_txn_id : active_set_) {
        *txn_id = std::max(*txn_id, active_txn_id);
      }
      active_set_.clear();
      return true;
    }
    active_set_.clear();  // 每次dfs之前都需要清空active_set
  }
  return false;
}
/*
* GetEdgeList: 返回图中所有边的列表
*/
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  // 遍历waits_for中的所有pair, 返回所有t1->t2
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for(auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for(auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}
/*
* RunCycleDetection: 后台运行循环死锁检测的框架代码
*/
void LockManager::RunCycleDetection() {
  // 在enable_cycle_detection标志位有效的情况下, 间隔cycle_detection_interval的时间, 进行一轮检测
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();   // 事先获取表/行锁的map的latch
      // 1. 检测表锁请求队列
      // 1.1 遍历所有表锁请求队列
      for(auto &pair : table_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock(); //对每个表锁, 先拿锁
        // 1.1.1 遍历每个表锁的所有lock request
        for(auto const &lock_request : pair.second->request_queue_) {
          if(lock_request->granted_) {
            // 1.1.1.1 如果锁已经授予, 将TX ID添加到granted_set
            granted_set.emplace(lock_request->txn_id_);
          }else {
            // 1.1.1.2 如果锁未被授予, 将该请求添加到等待图, 并未每个已授予的事务添加一条边, 表示该请求在等待这些事务
            for(auto txn_id : granted_set) {
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock(); // 最后别忘了释放每个表锁的锁
      }
      // 2. 检测行锁请求队列
      // 2.1 遍历所有行锁请求队列
      for(auto &pair : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();     //对每个行锁先拿锁
        // 2.1.1 遍历每个行锁的lock request
        for(auto const &lock_request : pair.second->request_queue_) {
          if(lock_request->granted_) {
            // 2.1.1.1 如果锁已经授予, 将TX ID添加到granted_set
            granted_set.emplace(lock_request->txn_id_);
          }else {
            // 2.1.1.2 如果锁未被授予, 将该请求添加到等待图, 并未每个已授予的事务添加一条边, 表示该请求在等待这些事务
            for(auto txn_id : granted_set) {
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();   //每个行锁记得释放锁
      }
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock(); // 用完了可以释放前面获取的表/行锁的map的锁
      // 3. 检测并处理死锁
      txn_id_t txn_id;
      // 3.1 HasCycle 利用 Dfs 检测是否存在死锁
      while(HasCycle(&txn_id)) {
        // 3.1.1 如果存在死锁, 获取txn_id最大(最新)的事务, 将其状态设置为ABORTED, 并删除等待图中的节点
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        DeleteNode(txn_id);
        // 3.1.2 释放涉及死锁的事务, 并通知相关锁请求队列
        if(map_txn_oid_.count(txn_id) > 0) {
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        } 
        if (map_txn_rid_.count(txn_id) > 0) {
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
        }
      }
      // 4. 清理临时数据结构, 进行下一次死锁检测
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}
/*
* GrantLock: 判断是否可以授予给定的锁请求
*/
auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto &lr : lock_request_queue->request_queue_) {
    // 1. 遍历lock request queue找到已被授予的锁lr->granted_, 检查当前模式和要授予的锁模式
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {
        // 1.1 要授予共享锁, 如果当前锁的状态是以下三种之一, 返回false(总结就是存在意向锁和其他表级写锁)
        // INTENTION_EXCLUSIVE: 意向排他锁, 事务在某些行上获取了写锁
        // SHARED_INTENTION_EXCLUSIVE: 共享意向排他锁, 事务在行上获取了读写锁
        // EXCLUSIVE: 当前表已经被获取了写锁, 有其他的写操作正在进行
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        // 1.2 要授予排他锁: 当前如果存在任何锁都不行, 因为是独占的
        case LockMode::EXCLUSIVE:
          return false;
          break;
        // 1.3 要授予意向共享锁, 只有当前存在排他锁才会不兼容
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        // 1.4 要授予意向排他锁, 存在共享锁, 共享意向排他锁, 排他锁会失败
        // SHARED: 共享锁, 有人在读表, 不允许修改表
        // SHARED_INTENTION_EXCLUSIVE: 共享意向排他, 允许读, 但意向排他
        // EXCLUSIVE: 排他锁, 都不兼容
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        // 1.5 要授予共享意向排他锁, 允许读但事务也希望在其他行上获取排他锁
        // 只有当前锁是INTENTION_SHARED意向共享才可以授予
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    // 2. 对于未授的锁请求, 如果该请求不是当前正在检查的锁请求, 返回false
    } else if (lock_request.get() != lr.get()) {
      return false;
    // 3. 如果该请求是当前正在检查的锁请求, 返回true, 可以授予锁
    } else {
      return true;
    }
  }
  return false;
}
/* 
* InsertOrDeleteTableLockSet: 根据锁模式, 插入或删除请求对应的表id从锁集合中删除
*/
void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  switch (lock_request->lock_mode_) {
    // 根据锁的lock mode, 插入/删除到对应的LockSet
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}
/*
* InsertOrDeleteRowLockSet: 根据锁模式, 插入或删除请求对应的表id从锁集合中删除
*/
void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  // 根据锁的lock mode, 插入/删除到对应的LockSet
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

auto LockManager::Dfs(txn_id_t txn_id) -> bool {
  // End Condition: 如果是已经遍历的txn_id, 说明都遍历过了, 没有环
  if (safe_set_.find(txn_id) != safe_set_.end()) {
    return false;
  }
  active_set_.insert(txn_id);
  // DFS range: 遍历txn_id -> 指向的所有边
  std::vector<txn_id_t> &next_node_vector = waits_for_[txn_id];
  std::sort(next_node_vector.begin(), next_node_vector.end());
  for(txn_id_t const next_node : next_node_vector) {
    // 如果active_set中已经存在了, 说明出现了环
    if(active_set_.find(next_node) != active_set_.end()) {
      return true;
    }
    if(Dfs(next_node)) {
      return true;
    }
  }

  active_set_.erase(txn_id);  // 回溯
  safe_set_.insert(txn_id);
  return false;
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  // 1. 删掉等待图waits_for_中
  waits_for_.erase(txn_id);
  // 2. 调用RemoveEdge在边集中删除所有txn_id为起点的边
  for(auto a_txn_id : txn_set_) {
    if(a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}

}  // namespace bustub

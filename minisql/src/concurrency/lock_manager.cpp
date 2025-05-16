#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) 
{
    // Thread safe
    std::unique_lock<std::mutex> unique_lock(latch_);

    // If the isolation level is read uncommitted, set abort
    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted)
    {
        // This isolation level can not be added S lock
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }

    LockPrepare(txn, rid);
    LockRequestQueue &lock_request_queue = lock_table_[rid];
    lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);  // add request

    // If has X lock, wait until two condition
    if (lock_request_queue.is_writing_) 
    {
        lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
          return txn->GetState() == TxnState::kAborted || !lock_request_queue.is_writing_;
        });
    }
    CheckAbort(txn, lock_request_queue);  // check if it is aborted
    // cnt++, get set the S lock
    txn->GetSharedLockSet().emplace(rid);
    lock_request_queue.sharing_cnt_++;
    auto iter = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    iter->granted_ = LockMode::kShared;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> unique_lock(latch_);
    // prepare, and add one X lock to queue
    LockPrepare(txn, rid);
    LockRequestQueue &lock_request_queue = lock_table_[rid];
    lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);

    // If has X or S lock, wait
    if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 0)
    {
        lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
            return txn->GetState() == TxnState::kAborted || (!lock_request_queue.is_writing_ && lock_request_queue.sharing_cnt_ == 0);
        });
    }
    CheckAbort(txn, lock_request_queue);
    // Get the X lock
    txn->GetExclusiveLockSet().emplace(rid);
    lock_request_queue.is_writing_ = true;
    auto it = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    it->granted_ = LockMode::kExclusive;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> unique_lock(latch_);

    // If in the shrinking phase, upgrade leads to aborted and exception
    if (txn->GetState() == TxnState::kShrinking)
    {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }

    LockRequestQueue &lock_request_queue = lock_table_[rid];
    // If there is transaction upgrading, abort and throw
    if (lock_request_queue.is_upgrading_)
    {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
    }

    // If has had X lock, no need to upgrade
    auto it = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    if (it->lock_mode_ == LockMode::kExclusive && it->granted_ == LockMode::kExclusive)
        return true;

    it->lock_mode_ = LockMode::kExclusive;
    it->granted_ = LockMode::kShared;
    
    // if there exists X lock or has other transaction sharing, cannot upgrade, wait
    if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 1) 
    {
        lock_request_queue.is_upgrading_ = true;
        lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
          return txn->GetState() == TxnState::kAborted || (!lock_request_queue.is_writing_ && lock_request_queue.sharing_cnt_ == 1);
        });
    }
    
    // If aborted 
    if (txn->GetState() == TxnState::kAborted)
        lock_request_queue.is_upgrading_ = false;
    CheckAbort(txn, lock_request_queue);
    
    txn->GetSharedLockSet().erase(rid);      // delete S lock
    txn->GetExclusiveLockSet().emplace(rid); // add X lock
    lock_request_queue.sharing_cnt_--;       // S lock --   
    lock_request_queue.is_upgrading_ = false;
    lock_request_queue.is_writing_ = true;
    it->granted_ = LockMode::kExclusive;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> unique_lock(latch_);
    LockRequestQueue &lock_request_queue = lock_table_[rid];
    // delete X or S lock
    txn->GetSharedLockSet().erase(rid);
    txn->GetExclusiveLockSet().erase(rid);

    auto it = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    auto lock_mode = it->lock_mode_;

    if (! lock_request_queue.EraseLockRequest(txn->GetTxnId()))
        return false;
    // Growing phase and not RC level unlocking S lock
    if(txn->GetState() == TxnState::kGrowing && !(txn->GetIsolationLevel() == IsolationLevel::kReadCommitted && lock_mode == LockMode::kShared))
        txn->SetState(TxnState::kShrinking);
    
    // unlock S lock or X lock, all need to notify others 
    if(lock_mode == LockMode::kShared)
    {
        lock_request_queue.sharing_cnt_--;
        lock_request_queue.cv_.notify_all();
    }
    else
    {
        lock_request_queue.is_writing_ = false;
        lock_request_queue.cv_.notify_all();
    }

    return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) 
{
    // Before add X or S lock, should call this function
    // If shrinking phase, abort
    if (txn->GetState() == TxnState::kShrinking)
    {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }

    // If not exists, create new
    if (lock_table_.find(rid) == lock_table_.end())
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) 
{
    if (txn->GetState() == TxnState::kAborted)
    {
        req_queue.EraseLockRequest(txn->GetTxnId());
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) 
{
    // t1 -> t2
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) 
{
    waits_for_[t1].erase(t2);  
}

// For searching for cycles, add one DFS function
bool LockManager::DFS(const txn_id_t txn_id)
{
    if (visited_set_.count(txn_id))
    {
        // Already exists, has cycle(twice)
        revisited_node_ = txn_id;
        return true;
    }

    // Add to path
    visited_set_.insert(txn_id);
    visited_path_.push(txn_id);
    
    // Search recursively
    for (auto wait_id : waits_for_[txn_id])
        if (DFS(wait_id))
            return true;
    
    // Detele from path, search back
    visited_set_.erase(txn_id);
    visited_path_.pop();
    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) 
{
    // clear path
    revisited_node_ = INVALID_TXN_ID;
    visited_set_.clear();
    while (!visited_path_.empty())
        visited_path_.pop();
    
    // A set to collect all the txn, ensure the oldest txn deletes first
    std::set<txn_id_t> txn_set;
    for (auto [t1, neighbor] : waits_for_)
    {
        if (!neighbor.empty())       // has neighbor, insert into set
        {
            txn_set.insert(t1);
            for (auto t2 : neighbor)
                txn_set.insert(t2);
        }
    }

    // starts from each txn to search for cycle
    for (auto start_id : txn_set)
    {
        if (DFS(start_id))  // has cycle
        {
            // Find the oldest node, starting from revisited_node
            newest_tid_in_cycle = revisited_node_;
            // Stop when finish the cycle
            while (!visited_path_.empty() && revisited_node_ != visited_path_.top())
            {
                newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
                visited_path_.pop();
            }
            // while sentence finished, has set the newest_tid, return with true
            return true;
        }
    }

    // If the condition not meeted, no cycles
    return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() 
{
   // If enable, always check
   while (enable_cycle_detection_)
   {
        // wait for some time
        std::this_thread::sleep_for(cycle_detection_interval_);
        // construct and destruct variable
        {
            std::unique_lock<std::mutex> l(latch_);                // unique lock
            std::unordered_map<txn_id_t, RowId> required_rec;
            waits_for_.clear();                                    // clear original wait

            // build graph
            for (const auto &[row_id, lock_req_queue] : lock_table_)
            {
                for (auto lock_req : lock_req_queue.req_list_)
                {
                    if (lock_req.granted_ != LockMode::kNone)
                        continue;         // If has lock, no need to consider
                    required_rec[lock_req.txn_id_] = row_id;
                    for (auto granted_req : lock_req_queue.req_list_)
                        if (granted_req.granted_ != LockMode::kNone)
                            AddEdge(lock_req.txn_id_, granted_req.txn_id_);
                }
            }
            // unlock each cycle
            txn_id_t txn_id = INVALID_TXN_ID;
            while (HasCycle(txn_id))               // This function set the transaction to be aborted
            {
                auto txn = txn_mgr_->GetTransaction(txn_id);
                DeleteNode(txn_id);
                txn->SetState(TxnState::kAborted); // set aborted
                lock_table_[required_rec[txn_id]].cv_.notify_all();
            }
        } 
   } 
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() 
{
    std::vector<std::pair<txn_id_t, txn_id_t>> result;

    // Add each edge by traverse the graph
    for (auto [t1, neighbor] : waits_for_)
        for (auto t2 : neighbor)
            result.emplace_back(t1, t2);
    // sort result
    std::sort(result.begin(), result.end());
    return result;
}

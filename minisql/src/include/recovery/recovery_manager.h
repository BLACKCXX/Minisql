#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"
using namespace  std;



using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
      persist_lsn_ = last_checkpoint.checkpoint_lsn_;
      active_txns_ = move(last_checkpoint.active_txns_);
      data_ = move(last_checkpoint.persist_data_);
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
      for (auto log = log_recs_[persist_lsn_]; log->type_ != LogRecType::kInvalid ;log = log_recs_[++persist_lsn_] ) {
        //persist_lsn_++;
        if (log->type_ == LogRecType::kInvalid) break;
        active_txns_[log->txn_id_] = log->lsn_;
        switch (log->type_) {
          case (LogRecType::kInsert):
            data_[log->insert_key] = log->insert_val;
            break;
          case (LogRecType::kUpdate):
            //data_.erase(log->old_key);
            data_[log->new_key] = log->new_val;
            break;
          case (LogRecType::kDelete):
            data_.erase(log->delete_key);
            break;
          case(LogRecType::kBegin):
            active_txns_[log->txn_id_] = log->lsn_;
            break;
          case (LogRecType::kCommit):
            active_txns_.erase(log->txn_id_);
            break;
          case (LogRecType::kAbort):

            for (auto rollback = log_recs_[log->prev_lsn_]; rollback->prev_lsn_!=INVALID_LSN; rollback = log_recs_[rollback->prev_lsn_]) {
              switch (rollback->type_) {
                case (LogRecType::kInsert):
                  data_.erase(rollback->insert_key);
                  break;
                case (LogRecType::kUpdate):
                  //data_.erase(rollback->new_key);
                  data_[rollback->old_key] = rollback->old_val;
                  break;
                case (LogRecType::kDelete):
                  data_[rollback->delete_key] = rollback->delete_val;
                  break;
                default:break;

              }
            }
            active_txns_.erase(log->txn_id_);
            break;
            default:
            break;

        }
          if (persist_lsn_ >= (lsn_t)log_recs_.size() - 1)
            break;

      }

    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
     for (auto i : active_txns_) {
       for (auto rollback = log_recs_[i.second] ; rollback != nullptr ; rollback = log_recs_[rollback->prev_lsn_]  ) {
         switch (rollback->type_) {
           case (LogRecType::kInsert):
             data_.erase(rollback->insert_key);
             break;
           case (LogRecType::kUpdate):
             data_.erase(rollback->new_key);
             data_[rollback->old_key] = rollback->old_val;
             break;
           case (LogRecType::kDelete):
             data_[rollback->delete_key] = rollback->delete_val;
             break;
           default:break;
         }
         if (rollback->prev_lsn_ == INVALID_LSN) break;
       }

     }
active_txns_.clear();
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H

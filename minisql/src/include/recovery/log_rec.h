#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"
using namespace std;
enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};
// the type of locrec
// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */

struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    KeyType insert_key , delete_key , old_key, new_key;
    ValType insert_val{} , old_val{} , new_val{} , delete_val{};
  LogRec(LogRecType type,lsn_t lsn,lsn_t prev_lsn,txn_id_t
  txn_id):
    type_(type),lsn_(lsn),prev_lsn_(prev_lsn),txn_id_(txn_id)
  {}
    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsn_prev = INVALID_LSN;
  if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
     lsn_prev = LogRec::prev_lsn_map_[txn_id];
     LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }else {
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  LogRec log = LogRec(LogRecType::kInsert , LogRec::next_lsn_++, lsn_prev, txn_id);
  log.insert_key = move(ins_key);
  log.insert_val = ins_val;
  return std::make_shared<LogRec>(log);

}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t lsn_prev = INVALID_LSN;
  if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
    lsn_prev = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }else {
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  LogRec log = LogRec(LogRecType::kDelete , LogRec::next_lsn_++, lsn_prev, txn_id);
  log.delete_key = move(del_key);
  log.delete_val = del_val;
  return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t lsn_prev = INVALID_LSN;
    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
      lsn_prev = LogRec::prev_lsn_map_[txn_id];
      LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    }else {
      LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
    }
  LogRec log = LogRec(LogRecType::kUpdate , LogRec::next_lsn_++, lsn_prev, txn_id);
  log.old_key = move(old_key);
  log.old_val = old_val;
  log.new_key = move(new_key);
  log.new_val = new_val;
  return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t lsn_prev = INVALID_LSN;
  if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
    lsn_prev = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }else {
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  LogRec log = LogRec(LogRecType::kBegin, LogRec::next_lsn_++, lsn_prev, txn_id);
  return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsn_prev = INVALID_LSN;
  if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
    lsn_prev = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }else {
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  LogRec log = LogRec(LogRecType::kCommit, LogRec::next_lsn_++, lsn_prev, txn_id);
  return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsn_prev = INVALID_LSN;
  if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
    lsn_prev = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }else {
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  LogRec log = LogRec(LogRecType::kAbort, LogRec::next_lsn_++, lsn_prev, txn_id);
  return std::make_shared<LogRec>(log);
}

#endif  // MINISQL_LOG_REC_H

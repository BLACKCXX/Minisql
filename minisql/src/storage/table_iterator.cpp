#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
 table_heap_ = table_heap;
  txn_ = txn;
  row_id_ = rid;
}

TableIterator::TableIterator(const TableIterator &other) {
table_heap_ = other.table_heap_;
  txn_ = other.txn_;
  row_id_ = other.row_id_;
}

TableIterator::~TableIterator() {
  delete txn_;
  delete table_heap_;
  delete txn_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (row_id_ != itr.row_id_) return false;
  if (txn_ != itr.txn_) return false;
  if (table_heap_ != itr.table_heap_) return false;
  return true;

}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  Row row;
  row.SetRowId(row_id_);
  table_heap_->GetTuple(&row , txn_);
  ASSERT(&row, "Invalid row.");
  return row;
}

Row *TableIterator::operator->() {
  Row *ptr_row;
  ptr_row->SetRowId(row_id_);
  table_heap_->GetTuple(ptr_row, txn_);
  ASSERT(ptr_row, "Invalid row.");
  return ptr_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  txn_ = itr.txn_;
  row_id_ = itr.row_id_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  Row row;
  row.SetRowId(row_id_);
  table_heap_->GetTuple(&row, txn_);
  RowId next_row_id = table_heap_->GetNextTupleID(&row , txn_);
  row_id_ = next_row_id;
  return *this;
}

// iter++
TableIterator& TableIterator::operator++(int) {
  Row row;
  row.SetRowId(row_id_);
  table_heap_->GetTuple(&row, txn_);
  RowId next_row_id = table_heap_->GetNextTupleID(&row , txn_);
  row_id_ = next_row_id;

  return *this;
}

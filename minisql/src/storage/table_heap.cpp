#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  /**
    * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
    * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
    * @param[in] txn The recovery performing the insert
    * @return true iff the insert is successful
    */
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size > PAGE_SIZE) {
    return false;
  }
  // Find the  page
  page_id_t page_id = first_page_id_;
  page_id_t page_id_prev = INVALID_PAGE_ID;
  bool found = false;
  while (found == false) {
    if (page_id == INVALID_PAGE_ID) {
      buffer_pool_manager_->NewPage(page_id);
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      page->WLatch();
      page->Init(page_id , page_id_prev , log_manager_ , txn );
      found =  page->InsertTuple(row , schema_ , txn , lock_manager_ , log_manager_);


    }
    if (page_id == INVALID_PAGE_ID) {
      first_page_id_ = page_id;
    }

  }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {


  // if it is too larghe


}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { return false; }

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(nullptr, RowId(), nullptr); }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }

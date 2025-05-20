#include "storage/table_heap.h"
#include <cassert>
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
  /*
    uint32_t tuple_size = row.GetSerializedSize(schema_);
    if (tuple_size > PAGE_SIZE) {
      return false;
    }
    // Find the  page
    page_id_t page_id = first_page_id_;
    page_id_t page_id_prev = INVALID_PAGE_ID;
    bool found = false;
    while (found == false) {
      if (page_id == INVALID_PAGE_ID) {  // if the page_id is invalid then we should build an new page
        buffer_pool_manager_->NewPage(page_id);
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));// create an new page
        if (page == nullptr) {
          return false;
        }
        page->WLatch();
        page->Init(page_id , page_id_prev , log_manager_ , txn );
        found =  page->InsertTuple(row , schema_ , txn , lock_manager_ , log_manager_);
        if (page_id_prev == INVALID_PAGE_ID) {
          first_page_id_ = page_id;
        }else {
          auto page_prev = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id_prev));
          page_prev->WLatch();
          page_prev->SetNextPageId(page_id);
          buffer_pool_manager_->UnpinPage(page_id_prev ,  true);

        }
        page->WLatch();
        buffer_pool_manager_->UnpinPage(page_id ,  true);
        break;


      }else {
        // 如果page 已经存在
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));// create an new page
        if (page == nullptr) {
          return false;
        }
        page->WLatch();
        found = page->InsertTuple(row , schema_ , txn , lock_manager_ , log_manager_);
        page_id_prev = page_id;
        page_id = page->GetNextPageId();
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id ,  true);
      }
    }
    return found;*/
  for (auto page_id = first_page_id_; page_id != INVALID_PAGE_ID; ) {

    // fetch page from buffer pool by page_id
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

    if (page == nullptr)
      return false;
    page->WLatch();

    auto next_page_id = page->GetNextPageId();
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    } else {
      page->WUnlatch();
    }

    // If the page is full, then create a new page.
    if (next_page_id == INVALID_PAGE_ID) {
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      if (new_page == nullptr)
        return false;

      page->WLatch();
      new_page->WLatch();

      new_page->Init(new_page->GetPageId(), page_id, log_manager_, txn);
      new_page->SetPrevPageId(page_id);
      page->SetNextPageId(new_page_id);
      new_page->SetNextPageId(INVALID_PAGE_ID);

      page->WUnlatch();
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page_id, true);
    } else {
      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = next_page_id;
    }
  }

  return false;
}

RowId TableHeap::GetNextTupleID(Row *row, Txn *txn) {
  Row* current_row = row;
  RowId R_ID = row->GetRowId();
  RowId return_id = INVALID_ROWID;

  bool found = false;

  page_id_t cur_page_id = row->GetRowId().GetPageId();
  auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
  if (cur_page == nullptr) {
    return INVALID_ROWID;
  }else {
    cur_page->RLatch();
    found = cur_page->GetNextTupleRid(R_ID , &return_id);
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_id ,  false);
    if (found == true) {
      return return_id;
    }
  }
  page_id_t next_page_id = cur_page->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return INVALID_ROWID;
  }
  while(found == false) {
    if (next_page_id == INVALID_PAGE_ID) {
      return INVALID_ROWID;
    }
    auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    if (next_page == nullptr) {
      return INVALID_ROWID;
    }
    next_page->RLatch();
    found = next_page->GetFirstTupleRid( &return_id);
    next_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(next_page_id ,  false);
    next_page_id = next_page->GetNextPageId();

  }
  if (found == true) {
    return return_id;
  }else return INVALID_ROWID;


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
  // Step1: Find the page which contains the tuple.
  // Step2: Update the tuple in the page.
  // Step3: Unpin the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr)
    return false;
  page->WLatch();
  Row old_row(rid);

  TablePage::UpdateStatus status = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (status == TablePage::UpdateStatus::updateSuccess) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
  } else if (status == TablePage::UpdateStatus::notEnoughSpace) {
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    if (InsertTuple(row, txn)) {
      return true;
    } else {
      return true;
    }
  } else {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Step3: Unpin the page.

  // find the page which contains the tuple
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(ERROR) << "Page not found" << "in TableHeap::ApplyDelete()" <<std::endl;
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
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
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Read the tuple from the page.
  // Step3: Unpin the page.

  // Fetch func will pin a page
  RowId rid = row->GetRowId();
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    LOG(ERROR) << "Page not found" << std::endl;
    return false;
  }

  page->RLatch();
  bool ret = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();

  if (!ret) {
    buffer_pool_manager_->UnpinPage(page_id,false);
    LOG(ERROR) << "GetTuple failed" << std::endl;
    return false;
  } else {
    buffer_pool_manager_->UnpinPage(page_id,false);
    return ret;
  }

}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
// TableIterator TableHeap::Begin(Txn *txn) {
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//   if (page == nullptr) {
//     return this->End();
//   }
//   RowId first_rid;
//   page->RLatch();
//   if (page->GetFirstTupleRid(&first_rid)) {
//     Row *row = new Row(first_rid);
//     page->GetTuple(row, schema_, txn, lock_manager_);
//     page->RUnlatch();
//     buffer_pool_manager_->UnpinPage(first_page_id_, false);
//     return TableIterator(this, *row, txn);
//   }
//   page->RUnlatch();
//   buffer_pool_manager_->UnpinPage(first_page_id_, false);
//   return this->End();
// }
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;
  RowId result_rid;
  while(1)
  {
    if(page_id == INVALID_PAGE_ID)
    {
      return End();
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page->GetFirstTupleRid(&result_rid))
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = page->GetNextPageId();
  }
  if(page_id != INVALID_PAGE_ID)
  {
    Row* result_row = new Row(result_rid);
    GetTuple(result_row, txn);
    return TableIterator(this, *result_row, txn);
  }
  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  RowId rid(INVALID_PAGE_ID, 0);
  return TableIterator(this, rid, nullptr);
}

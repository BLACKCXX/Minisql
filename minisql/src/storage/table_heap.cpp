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
  bool inserted = false;
  page_id_t current_page_id = first_page_id_;
  page_id_t prev_page_id = INVALID_PAGE_ID;

  while (!inserted) {
    if (current_page_id == INVALID_PAGE_ID) {
      // 创建新页面并插入
      Page *new_page = buffer_pool_manager_->NewPage(current_page_id);
      if (new_page == nullptr) return false;  // 处理分配失败

      auto new_table_page = reinterpret_cast<TablePage *>(new_page->GetData());
      new_table_page->WLatch();
      new_table_page->Init(current_page_id, prev_page_id, log_manager_, txn);
      inserted = new_table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      new_table_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, true);

      // 更新链表
      if (prev_page_id != INVALID_PAGE_ID) {
        auto prev_page = buffer_pool_manager_->FetchPage(prev_page_id);
        auto prev_table_page = reinterpret_cast<TablePage *>(prev_page->GetData());
        prev_table_page->WLatch();
        prev_table_page->SetNextPageId(current_page_id);
        prev_table_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page_id, true);
      } else {
        first_page_id_ = current_page_id;  // 更新首页面
      }
      break;  // 无论插入是否成功，退出循环
    } else {
      // 尝试插入当前页面
      auto current_page = buffer_pool_manager_->FetchPage(current_page_id);
      auto current_table_page = reinterpret_cast<TablePage *>(current_page->GetData());
      current_table_page->WLatch();
      inserted = current_table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      current_table_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, inserted);

      if (!inserted) {
        // 移动到下一页
        prev_page_id = current_page_id;
        current_page_id = current_table_page->GetNextPageId();
      }
    }
  }
  return inserted;
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
  // if it is too larghe
  if (row.GetSerializedSize(schema_) > PAGE_SIZE) {
    return false;
  }
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  old_page->WLatch();
  Row old_row = Row(rid);
  bool result = old_page->UpdateTuple(row ,&old_row , schema_ , txn , lock_manager_ , log_manager_);
  old_page->WLatch();
  buffer_pool_manager_->UnpinPage(old_page->GetTablePageId(), true);
  return result;
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
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  bool found = false;
  /*page_id_t page_id = first_page_id_;
  page_id_t page_id_prev = INVALID_PAGE_ID;
  while (found == false) {
    if (page_id == INVALID_PAGE_ID) {
      return false; // there is no chance to find it
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
      return false;
    }
    page->RLatch();
    found = page->GetTuple(row , schema_ , txn ,  lock_manager_ );
    page->RUnlatch();


  }
  return found;*/
  RowId rid = row->GetRowId();
  if (rid == INVALID_ROWID) return false;
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) return false;
  page->RLatch();
  found = page->GetTuple(row , schema_ , txn , lock_manager_ );
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return found;
}

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
 * TODO: Student Imple
 *
 *
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;
  bool found = false;
  RowId rid;
  while (found == false) {
   if (page_id == INVALID_PAGE_ID) {
     return TableIterator(nullptr, RowId(), nullptr);
   } else {
     auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
     page->RLatch();
     //assert(page != nullptr , "page  is the nullptr");
     if (page->GetFirstTupleRid(&rid)) {
       found = true;
       page->RUnlatch();
       buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
     }else{
       page->RUnlatch();
       buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
       page_id = page->GetNextPageId();
     }
   }

  }return TableIterator(this,rid , txn);

}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, RowId(), nullptr);
} // 末尾指针本来就没啥需要做的

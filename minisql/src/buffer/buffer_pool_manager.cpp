#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id_temp;
  if (page_id >= MAX_VALID_PAGE_ID || page_id <= INVALID_PAGE_ID) {
    return nullptr;
  }
  bool found = false;
  // search the table to get the requested page
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if (it->first == page_id) {
      frame_id_temp = it->second;
      found = true;
      break;
    }
  }
  // if P exisit pin it  and  return it immediately
 if (found == true) {
   frame_id_temp = page_table_[page_id];
   replacer_->Pin(frame_id_temp);
   //pin it
   pages_[frame_id_temp].pin_count_ = 1;
   return &pages_[frame_id_temp];
 }
  //If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if (free_list_.size() == 0 && replacer_->Size() == 0) {
    return nullptr;
  }
  else if (free_list_.size() > 0) {
    frame_id_temp = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_id_temp; // insert in to it
    disk_manager_->ReadPage(page_id , pages_[frame_id_temp].data_);
    //replacer_->Pin(frame_id_temp);
    pages_[frame_id_temp].pin_count_ = 1;
    pages_[frame_id_temp].page_id_ = page_id;
    return &pages_[frame_id_temp];
  }
  else {
   if ( replacer_->Victim(&frame_id_temp) == false)
      return nullptr; // get the newline


    page_id_t old_page_id = pages_[frame_id_temp].page_id_;
    if (old_page_id != INVALID_PAGE_ID) {
      page_table_.erase(old_page_id); // 删除旧映射
      }
    if (pages_[frame_id_temp].is_dirty_ == true) {
      disk_manager_->WritePage(pages_[frame_id_temp].GetPageId() , pages_[frame_id_temp].GetData());

    }
    pages_[frame_id_temp].page_id_ = page_id;
    pages_[frame_id_temp].pin_count_ = 1;
    page_table_[page_id] = frame_id_temp;
    disk_manager_->ReadPage(page_id , pages_[frame_id_temp].data_);
    return &pages_[frame_id_temp];
  }
  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // all things are pined
  frame_id_t frame_id_temp;
  if (free_list_.size() == 0 && replacer_->Size() == 0) {
    return nullptr;
  }else if (free_list_.size() > 0) {
    frame_id_temp = free_list_.front();
    free_list_.pop_front();
  }else {
    if (replacer_->Victim(&frame_id_temp) == false)
      return nullptr;
    if (pages_[frame_id_temp].IsDirty() == true) {
      disk_manager_->WritePage(pages_[frame_id_temp].GetPageId() , pages_[frame_id_temp].data_);
      pages_[frame_id_temp].is_dirty_ = false;
    }
   page_table_.erase(pages_[frame_id_temp].GetPageId());
  }
  page_id = AllocatePage();
  pages_[frame_id_temp].ResetMemory();
  pages_[frame_id_temp].page_id_ = page_id;
  pages_[frame_id_temp].pin_count_ = 1;
  pages_[frame_id_temp].is_dirty_ = false;
  page_table_[page_id] = frame_id_temp;


  return &pages_[frame_id_temp];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  bool found = false;
  frame_id_t frame_id_temp;
  if (page_id == INVALID_PAGE_ID) {
    return  true;
  }
  if (page_id > MAX_VALID_PAGE_ID || page_id < INVALID_PAGE_ID) {
    return true;
  }
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if (it->first == page_id) {
      found = true;
      frame_id_temp = it->second;

      break;
    }
  }
  // iF P dosen't exit return true
  if (found == false) {
    return true;
  }else {
    if (pages_[frame_id_temp].pin_count_ > 0) {
      return false;
    }else {
      page_table_.erase(page_id);

      pages_[frame_id_temp].ResetMemory();
      pages_[frame_id_temp].page_id_ =  INVALID_PAGE_ID;
      pages_[frame_id_temp].is_dirty_ = false;
      // we can delete it
      free_list_.push_back(frame_id_temp);
      DeallocatePage(page_id);


      return true;

    }

  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id_temp = page_table_[page_id];
  if (pages_[frame_id_temp].GetPinCount() == 0) {
    return false;
  }
  pages_[frame_id_temp].pin_count_--;
  replacer_->Unpin(frame_id_temp);

  pages_[frame_id_temp].is_dirty_ = is_dirty;
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id_temp = page_table_[page_id];
  disk_manager_->WritePage(page_id , pages_[frame_id_temp].data_);
  pages_[frame_id_temp].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
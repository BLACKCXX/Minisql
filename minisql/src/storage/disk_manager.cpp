#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"
/*
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */

/*page_id_t DiskManager::AllocatePage() {
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;
  //tomuch
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t free = 0;
  for (free ; free < extent_num; ++free) {
    if (meta_page->extent_used_page_[free] < BITMAP_SIZE) {
      break;
    }
  }
  char page_data[PAGE_SIZE];
  page_id_t physical_id = free * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(physical_id, page_data);
  uint32_t page_offset = 0;
  auto bitmap_page = reinterpret_cast<BitmapPage <PAGE_SIZE>*>(page_data);
  if (!bitmap_page->AllocatePage(page_offset)) {
    LOG(ERROR) << "Failed to allocate bitmap page.";
  }
  WritePhysicalPage(physical_id, page_data);
  meta_page->extent_used_page_[free] ++;
  if (meta_page->extent_used_page_[free] == 1 ) {
    meta_page->num_extents_ ++;
  }
  return free * BITMAP_SIZE + page_offset;
}
*/

/**
 * TODO: Student Implement
 */
/*void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t this_extent = logical_page_id / BITMAP_SIZE;
  page_id_t physical_id = (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1 ;// 对应的物理页号 bitmap 的物理页号
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_id, page_data); // read the information
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  if (!bitmap_page->DeAllocatePage(physical_id%BITMAP_SIZE)) {
    LOG(ERROR) << "Failed to deallocate bitmap page.";
  }else {
    WritePhysicalPage(physical_id, page_data); //write back
    meta_page->num_allocated_pages_ --;
    meta_page->extent_used_page_[this_extent] --;
  }

}

/**
 * TODO: Student Implement
 */
/*bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID)
    return false;

  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t this_extent= logical_page_id / BITMAP_SIZE;
  page_id_t physical_id = (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1 ;// 对应的bitmap 的物理页号
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_id, page_data);
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  if (!bitmap_page->IsPageFree(logical_page_id%BITMAP_SIZE)) {
    return false;
  }else {
    return true;
  }
}

/**
 * TODO: Student Implement
 */
/*page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;
  return (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1  + logical_page_id % BITMAP_SIZE + 1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}*/



DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  if (!db_io_.is_open()) {
    db_io_.clear();
    // 创建父目录
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) {
      std::filesystem::create_directories(p.parent_path());
    }
    // 创建新文件
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // 重新以读写模式打开
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}
/*
page_id_t DiskManager::AllocatePage() {
  //  ASSERT(false, "Not implemented yet.");
  //  return INVALID_PAGE_ID;
  uint32_t meta_data_uint[PAGE_SIZE/4];
  memcpy(meta_data_uint, meta_data_, 4096);

  size_t page_id;
  // 寻找第一个没有满额的extent
  uint32_t extent_id = 0;
  while (  *(meta_data_uint+2+extent_id) == BITMAP_SIZE) {
    extent_id++;
  };
  // 读取对应extent的bitmap_page，寻找第一个free的page
  char bitmap[PAGE_SIZE];
  page_id_t bitmap_physical_id = extent_id * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(bitmap_physical_id, bitmap);

  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  uint32_t next_free_page = bitmap_page->GetNextFreePage();
  page_id = extent_id * BITMAP_SIZE + next_free_page;

  bitmap_page->AllocatePage(next_free_page);
  // 修改meta_data
  if (extent_id >= *(meta_data_uint+1)) ++ *(meta_data_uint+1);
  ++ *(meta_data_uint+2+extent_id);
  ++ *(meta_data_uint);

  memcpy(meta_data_,meta_data_uint, 4096);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_physical_id, bitmap);
  return page_id;
}
*/

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t this_extent = logical_page_id / BITMAP_SIZE;
  page_id_t physical_id = (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1 ;// 对应的物理页号 bitmap 的物理页号
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_id, page_data); // read the information
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  if (!bitmap_page->DeAllocatePage(logical_page_id%BITMAP_SIZE)) {
    LOG(ERROR) << "Failed to deallocate bitmap page.";
  }else {
    WritePhysicalPage(physical_id, page_data); //write back
    meta_page->num_allocated_pages_ --;
    meta_page->extent_used_page_[this_extent] --;
  }

}


page_id_t DiskManager::AllocatePage() {
  auto const meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;
  //tomuch
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t free = 0;
  for (free ; free < extent_num; free++) {
    if (meta_page->extent_used_page_[free] < BITMAP_SIZE) {
      break;
    }
  }


  char page_data[PAGE_SIZE];
  page_id_t physical_id = free * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(physical_id, page_data);


  uint32_t page_offset = 0;
  auto bitmap_page = reinterpret_cast<BitmapPage <PAGE_SIZE>*>(page_data);
  if (!bitmap_page->AllocatePage(page_offset)) {
    LOG(ERROR) << "Failed to allocate bitmap page.";
  }
  WritePhysicalPage(physical_id, page_data);
  meta_page->extent_used_page_[free] ++;
  meta_page->num_allocated_pages_++;
  if (meta_page->extent_used_page_[free] == 1 ) {
    meta_page->num_extents_ ++;
  }
  return free * BITMAP_SIZE + page_offset;
}

/*
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //  ASSERT(false, "Not implemented yet.");
  char bitmap[PAGE_SIZE];
  size_t pages_per_extent = 1 + BITMAP_SIZE;
  page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / pages_per_extent;
  ReadPhysicalPage(bitmap_physical_id, bitmap);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);

  bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE);

  uint32_t meta_data_uint[PAGE_SIZE/4];
//   修改meta_data
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  if (-- *(meta_data_uint + 2 + extent_id) == 0) -- *( meta_data_uint + 1 );
  -- *(meta_data_uint);

  memcpy(meta_data_,meta_data_uint, 4096);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_physical_id, bitmap);
}*/

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // 判断对应的bitmap中那一bit是0还是1
  // 读取对应的bitmap
  char bitmap[PAGE_SIZE];
  size_t pages_per_extent = 1 + BITMAP_SIZE;
  page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / pages_per_extent;
  ReadPhysicalPage(bitmap_physical_id, bitmap);

  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  // 判断
  if (bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE)) return true;
  return false;
}

/*
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID)
    return false;

  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t this_extent= logical_page_id / BITMAP_SIZE;
  page_id_t physical_id = (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1 ;// 对应的bitmap 的物理页号
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_id, page_data);
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  if (!bitmap_page->IsPageFree(logical_page_id%BITMAP_SIZE)) {
    return false;
  }else {
    return true;
  }
}*/

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + 1 + 1 + logical_page_id / BITMAP_SIZE;
}
//page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
//if (logical_page_id >= MAX_VALID_PAGE_ID)
  //return INVALID_PAGE_ID;
//return (logical_page_id/BITMAP_SIZE ) *(BITMAP_SIZE + 1) + 1  + logical_page_id % BITMAP_SIZE + 1;
//}


int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
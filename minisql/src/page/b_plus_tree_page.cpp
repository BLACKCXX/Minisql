#include <cmath>                      // For ceil function
#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsLeafPage() const {
  if (page_type_ == IndexPageType :: LEAF_PAGE) // Determine leaf type
    return true;

  return false;
}

/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsRootPage() const {
  if (parent_page_id_ == INVALID_PAGE_ID) // Determine root type
    return true;

  return false;
}

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;    // Set type
}

int BPlusTreePage::GetKeySize() const {
  return key_size_;
}

void BPlusTreePage::SetKeySize(int size) {
  key_size_ = size;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
  return size_; // Current size (number)
}

void BPlusTreePage::SetSize(int size) {
  size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) {
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMaxSize() const {
  return max_size_;
}

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetMaxSize(int size) {
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
/**
 * TODO: Student Implement
 */

// Three scenarios: root, leaf, between(neither root nor leaf)
int BPlusTreePage::GetMinSize() const {
  int result = 0;
  // root scenario
  if (this->IsRootPage())
    result = 2;
  // leaf scenario
  else if (this->IsLeafPage())
    result = int(ceil(1.0 * (max_size_ - 1) / 2));
  // internal, not root not leaf
  else
    result = int(ceil(1.0 * max_size_ / 2));
  
  return result;
  // return 0;
}

/*
 * Helper methods to get/set parent page id
 */
/**
 * TODO: Student Implement
 */
page_id_t BPlusTreePage::GetParentPageId() const {
  return parent_page_id_; // Get parent page id
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
  page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  lsn_ = lsn;
}
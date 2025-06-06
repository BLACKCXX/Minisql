#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) 
{
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int left = 0;
  int right = GetSize() - 1;
  int mid = 0;
  int temp = 0;
  int result = -1;    // larger than all, insert in the end

  // typical binary search
  while(left <= right)
  {
    mid = (left + right) / 2;
    temp = KM.CompareKeys(KeyAt(mid), key);
    if (temp < 0)
      left = mid + 1;
    else if (temp > 0)
    {
      result = mid;
      right = mid - 1;
    }
    else
      return mid;
  }

  // If exact match isn't found, it returns the first index where KeyAt(i) > key
  return result;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int temp_size = GetSize();
  int inserted_index = KeyIndex(key, KM);
  // index == -1, then append at the end
  if (inserted_index == -1)
  {
    SetKeyAt(temp_size, key);
    SetValueAt(temp_size, value);
  }
  else 
  {
    if (KM.CompareKeys(KeyAt(inserted_index), key) == 0)
      return -1;      // remains unique
    // shift right and insert
    PairCopy(PairPtrAt(inserted_index + 1), PairPtrAt(inserted_index), temp_size - inserted_index);
    SetKeyAt(inserted_index, key);
    SetValueAt(inserted_index, value);
  }
 
  // size + 1 and return size
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  // This function is very similar to the function in internal page
  int temp_size = GetSize();
  int temp_number = temp_size / 2;
  int temp_start_index = 0;

  if (temp_size % 2 == 1) // odd
    temp_start_index = temp_size / 2 + 1;
  else                    // even
    temp_start_index = temp_size / 2;
  recipient->CopyNFrom(PairPtrAt(temp_start_index), temp_number);
  SetSize(temp_size - temp_number);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int start_index = GetSize();                 // the end
  PairCopy(PairPtrAt(start_index), src, size); // parameter: des(this), src, size
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  // Typical binary search
  int left = 0;
  int mid = 0;
  int right = GetSize() - 1;
  int temp = 0;
  while (left <= right)
  {
    mid = (left + right) / 2;
    temp = KM.CompareKeys(KeyAt(mid), key);
    if (temp < 0)
      left = mid + 1;
    else if (temp > 0)
      right = mid - 1;
    else
    {
      value = ValueAt(mid);     // store its corresponding value in input "value"
      return true;
    }
  } 
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int temp_size = GetSize();
  int left = 0;
  int right = temp_size - 1;
  int mid = 0;
  int temp = 0;
  int temp_index = -1;
  
  while (left <= right)
  {
    mid = (left + right) / 2;
    temp = KM.CompareKeys(KeyAt(mid), key);
    if(temp < 0)
      left = mid + 1;
    else if (temp > 0)
      right = mid - 1;
    else
    {
      temp_index = mid;
      break;
    }
  }

  if (temp_index == -1) // never find
    return GetSize();
  else
  {
    // shift left one place, the remove target will be replaced
    PairCopy(PairPtrAt(temp_index), PairPtrAt(temp_index + 1), temp_size - temp_index - 1);
    SetSize(temp_size - 1);
    return GetSize();
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize()); // start from 0, copy all
  recipient->SetNextPageId(GetNextPageId());     // update next page (right)
  SetSize(0);                                    // clear 
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  // In leaf node, there is no dummy key
  int temp_size = GetSize();
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  PairCopy(PairPtrAt(0), PairPtrAt(1), temp_size - 1); // shift left, replace the first one
  SetSize(temp_size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  // append to the end
  int temp_size = GetSize();    
  SetKeyAt(temp_size, key);
  SetValueAt(temp_size, value);
  SetSize(temp_size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int temp_size = GetSize();
  recipient->CopyFirstFrom(KeyAt(temp_size - 1), ValueAt(temp_size - 1));
  SetSize(temp_size - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int temp_size = GetSize();
  PairCopy(PairPtrAt(1), PairPtrAt(0), temp_size); // shift right, make room for the first one
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(temp_size + 1);
}
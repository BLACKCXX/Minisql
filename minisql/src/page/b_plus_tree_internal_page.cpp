#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
  SetSize(0); // Initialization with 0 pair
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1;
  int right = GetSize() - 1;
  int mid = 0;
  int result = INVALID_PAGE_ID;

  // Binary search
  while (left <= right)
  {
    mid = (left + right) / 2;
    if (KM.CompareKeys(KeyAt(mid), key) < 0)
      left = mid + 1;
    else if (KM.CompareKeys(KeyAt(mid), key) > 0)
      right = mid - 1;
    else 
      return ValueAt(mid);
  }
  // If not found in the binary search, we get the left
  result = ValueAt(left - 1); //left is the first index such that KeyAt(left) > key, so left - 1 gives the largest index where KeyAt(i) ≤ key
  return result;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);                // two keys, one empty
  SetValueAt(0, old_value);  // left value
  SetKeyAt(1, new_key);      // new key
  SetValueAt(1, new_value);  // right value
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int temp_index = ValueIndex(old_value);
  int temp_size = GetSize();
  GenericKey* temp_key = nullptr;
  page_id_t temp_value = 0;

  for (int i = temp_size - 1; i > temp_index; i--)
  {
    // From right to left, until encounter old_value
    // For each position, copy the key and value from i to i + 1
    temp_key = KeyAt(i);
    temp_value = ValueAt(i);
    SetKeyAt(i + 1, temp_key);
    SetValueAt(i + 1, temp_value);
  }

  // Now insert the new_key and new_value into the correct position: right after old_value
  SetKeyAt(temp_index + 1, new_key);
  SetValueAt(temp_index + 1, new_value);
  SetSize(temp_size + 1);
  
  return temp_size + 1; // one more
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int temp_size = GetSize();        // The original size
  int temp_number = temp_size / 2;  // half or half - 1
  int temp_start = 0;               // From where

  if (temp_size % 2 == 1)           // odd, left one more than half
    temp_start = temp_size / 2 + 1;
  else                              // even, exactly half
    temp_start = temp_size / 2;
  
  recipient->CopyNFrom(PairPtrAt(temp_start), temp_number, buffer_pool_manager);
  // remain half or half + 1
  SetSize(temp_size - temp_number);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int begin_index = GetSize();                  // from this begin copy
  PairCopy(PairPtrAt(begin_index), src, size);  // call the function above
  page_id_t temp_value = 0;
  
  // For every child, set their parent as "this"
  for (int i = 0; i < size; i++)
  {
    temp_value = ValueAt(begin_index + i);
    auto page = buffer_pool_manager->FetchPage(temp_value);
    if (page != nullptr)
    {
      auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
      node -> SetParentPageId(GetPageId());
      buffer_pool_manager -> UnpinPage(temp_value, true);
    }
  }
  // The size should be incremented
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int temp_size = GetSize();
  page_id_t temp_value = 0;
  GenericKey* temp_key = nullptr;
  
  if(index >= 0 && index < temp_size)   // ensure the range
  {
    // from index, all move left
    for (int i = index; i < temp_size - 1; i++)
    {
      temp_key = KeyAt(i + 1);
      temp_value = ValueAt(i + 1);
      SetValueAt(i, temp_value);
      SetKeyAt(i, temp_key);
    }
    SetSize(temp_size - 1);
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t temp_value = ValueAt(0); // get the value
  SetSize(0);                        // clear
  return temp_value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);    // copy 1
  recipient->CopyNFrom(PairPtrAt(1),GetSize()-1,buffer_pool_manager);    // copy size - 1
  buffer_pool_manager->DeletePage(GetPageId());                          // delete current page
}


/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,BufferPoolManager *buffer_pool_manager) {
  // Actually, this function do not maintain the correctness of parent node
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);  
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // To get the index to be append and excecute append 
  int next_index = GetSize();                         //  The index to insert new pair
  SetKeyAt(next_index, key);
  SetValueAt(next_index, value);
  
  // similar to the function above copyNfrom
  auto page = buffer_pool_manager->FetchPage(value);
  if (page != nullptr)
  {
    auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
  // one more node
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  if(size == GetMinSize() || recipient->GetSize() == recipient->GetMaxSize()) {
    LOG(ERROR) << "Move overflow / underflow" << endl;
    return;
  }
  // Key[0] will be moved to Key[1] later
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(KeyAt(size - 1), ValueAt(size - 1), buffer_pool_manager);
  // Change middle key by first key
  *middle_key = *KeyAt(size - 1);
  // Remove last pair
  Remove(size - 1);                                 
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, key);
  for(int i = GetSize(); i > 0; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetValueAt(0, value);
  auto page = buffer_pool_manager->FetchPage(value);
  if(page != nullptr) {
    auto node = reinterpret_cast<InternalPage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
  IncreaseSize(1);
}
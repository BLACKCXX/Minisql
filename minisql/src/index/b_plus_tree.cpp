#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
  int leaf_max_size, int internal_max_size)
: index_id_(index_id),
  buffer_pool_manager_(buffer_pool_manager),
  processor_(KM),
  leaf_max_size_(leaf_max_size),
  internal_max_size_(internal_max_size) {
  if(leaf_max_size_ == 0)
  leaf_max_size_ = LEAF_PAGE_SIZE;
  if(internal_max_size_ == 0)
  internal_max_size_ = INTERNAL_PAGE_SIZE;
  //initialize root_page_id_
  auto index_root_pages = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  if(index_root_pages->GetRootId(index_id_, &root_page_id)) {
  root_page_id_ = root_page_id;
  } else {
  root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if(!IsEmpty())        // not empty, need destroy
  {
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page != nullptr)
    {
      auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());              // b plus tree node
      if (node->IsLeafPage())                                                     // leaf node
      {
        // just delete this page
        auto temp_leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
        buffer_pool_manager_->DeletePage(current_page_id);
      }
      else
      {
        // recursively call this function in all children
        auto temp_internal_node = reinterpret_cast<InternalPage*>(page->GetData());
        for (int i = 0; i < temp_internal_node->GetSize(); i++)
          Destroy(temp_internal_node->ValueAt(i));
        buffer_pool_manager_->DeletePage(current_page_id);
      }
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    }
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) // no root
    return true;
  auto temp_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto temp_node = reinterpret_cast<BPlusTreePage*>(temp_page->GetData());
  if (temp_node->GetSize() == 0)        // has root, but empty
  {
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);      
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) 
{
  if(IsEmpty())
    return false;
  else    // not empty
  {
    auto temp_page = FindLeafPage(key, root_page_id_);
    auto temp_leaf_page = buffer_pool_manager_->FetchPage(temp_page->GetPageId());
    auto temp_leaf_node = reinterpret_cast<BPlusTreeLeafPage*>(temp_leaf_page->GetData());
    RowId temp_res;
    int flag = 0;                // flag == 0 not found
    temp_leaf_page->RLatch();    // when process page, latch it
    if (temp_leaf_node->Lookup(key, temp_res, processor_))
    {
      // It is found 
      flag = 1;
      result.emplace_back(temp_res);
    }
    else
      flag = 0;
    temp_leaf_page->RUnlatch();  // unlock it
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false); // only read
    if (flag == 1)
      return true;
    else
      return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) 
{
  // if empty, create a new tree
  if (IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }   
  // not empty, call the following function
  else
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) 
{
  auto page = buffer_pool_manager_->NewPage(root_page_id_);
  // has got page
  if (page)
  {
    auto node = reinterpret_cast<LeafPage*>(page->GetData());
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)); // entries number
    // initialize and actually insert
    node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    node->Insert(key, value, processor_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);       // has been modified
  }
  else
    LOG(ERROR) << "Out of memory" << std::endl;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) 
{
  if (IsEmpty())
   return false;
  auto temp_page = FindLeafPage(key, root_page_id_);
  auto page = buffer_pool_manager_->FetchPage(temp_page->GetPageId());
  auto leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  
  page->WLatch();            // write latch this page
  RowId target_rowid;
  if (leaf_node -> Lookup(key, target_rowid, processor_))
  {
    // already exists
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false); // has not modify
    return false;                                                   // can not insert
  }
  else
  {
    // do not exist, can insert
    int leaf_current_size = leaf_node->Insert(key, value, processor_);
    if (leaf_current_size >= leaf_max_size_) // need split
    {
      auto new_sibling = Split(leaf_node, transaction);
      InsertIntoParent(leaf_node, new_sibling->KeyAt(0), new_sibling, transaction); // insert the first key of sibling to parent
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), true); // has been modified
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) 
{
   // the old and new page
   auto old_page = buffer_pool_manager_->FetchPage(node->GetPageId());
   page_id_t new_page_id;
   auto new_page = buffer_pool_manager_->NewPage(new_page_id);
   
   if (new_page == nullptr) // not enough memory
   {
    LOG(ERROR) << "Out of memory" << std::endl;
    return nullptr;
   }
   else
   {
    auto new_node = reinterpret_cast<InternalPage*>(new_page->GetData());        // get new node
    new_node->SetPageType(IndexPageType::INTERNAL_PAGE);                         // is internal
    new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);        // modified
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);  // modified
    return new_node;
  }
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) 
{
  // mostly like the above function
   auto old_page = buffer_pool_manager_->FetchPage(node->GetPageId());
   page_id_t new_page_id;
   auto new_page = buffer_pool_manager_->NewPage(new_page_id);
   if (new_page == nullptr)
   {
    LOG(ERROR) << "Out of memory" << std::endl;
    return nullptr;
   }
   else
   {
    auto new_node = reinterpret_cast<LeafPage*>(new_page->GetData());
    new_node->SetPageType(IndexPageType::LEAF_PAGE);     // leaf page
    new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_node);
    // need sibling connection
    new_node->SetNextPageId(node->GetNextPageId()); // right
    node->SetNextPageId(new_page_id);               // left
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_node;
   }

}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) 
{
  // in this function, new_node means the sibling
  if (old_node->IsRootPage()) // the old root split
  {
    auto new_page = buffer_pool_manager_->NewPage(root_page_id_);
    auto new_root_node = reinterpret_cast<InternalPage*>(new_page->GetData());
    
    new_root_node->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId()); // this populate function just form the new root
    old_node->SetParentPageId(new_root_node->GetPageId());
    new_node->SetParentPageId(new_root_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    UpdateRootPageId(0);
  }
  else
  {
    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
    int parent_current_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if (parent_current_size > internal_max_size_)                                  // should split
    {
      auto new_parent_sibling = Split(parent_node, transaction);
      auto key = new_parent_sibling->KeyAt(0);
      InsertIntoParent(parent_node, key, new_parent_sibling, transaction);         // recursively call, propagate upward
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);             // modified
    }
    else // do not split
    {
      // just update parent page id
      old_node->SetParentPageId(parent_node->GetPageId());
      new_node->SetParentPageId(parent_node->GetPageId());
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true); // modified
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) 
{
  if (IsEmpty())
    return;
  auto temp_res_page = FindLeafPage(key, root_page_id_);                   // find the leaf page
  auto leaf_page = buffer_pool_manager_->FetchPage(temp_res_page->GetPageId()); // fetch the page
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  leaf_page->WLatch(); // write latch
  int leaf_node_oldsize = leaf_node->GetSize();
  int leaf_node_currentsize = leaf_node->RemoveAndDeleteRecord(key, processor_);

  if (leaf_node_currentsize < leaf_node_oldsize)
  {
    // successfully find and delete
    leaf_page->WUnlatch();
    bool flag = false;
    if (leaf_node_currentsize < leaf_node->GetMinSize())
    {
      // too small, need to do sth
      flag = CoalesceOrRedistribute(leaf_node, transaction);
    }
    if (!flag) // if do not coalesce or redistribute, should unpin
      buffer_pool_manager_->UnpinPage(temp_res_page->GetPageId(), true);
  }
  else // did not find
  {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(temp_res_page->GetPageId(), false); // do not find, so nothing changed
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>                   // template, works for both internal and leaf page
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (IsEmpty()) // do nothing if tree empty
    return false;
  if (node->GetSize() >= node->GetMinSize()) // not underfull
    return false;
  if (node->IsRootPage())                    // is the root
  {
    if (node->GetSize() == 1)                // the root only has one entry
    {
      if (AdjustRoot(node))                  // return value = 1 means delete
      {
        buffer_pool_manager_->DeletePage(node->GetPageId());
        return true;
      }
    }
    // node size > 1, no need to coalesce or redistribute
    return false;
  }

  auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage*>(parent_node_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId()); //Find the index of node in the parent’s value array

  if (node_index == 0) // only has right sibling
  {
    auto sibling_node = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(1)));
    
    // if can be merge
    if (node->IsLeafPage() ? 
    ( sibling_node->GetSize() + node->GetSize() < node->GetMaxSize())
    : (sibling_node->GetSize() + node->GetSize() <= node->GetMaxSize())) // because internals store 1 fewer keys than values, so it’s safer to be inclusive
    {
      bool flag = false;
      flag = Coalesce(node, sibling_node, parent_node, 1, transaction);
      if (!flag)
        buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);  //  The separating key should be updated
      return false; // still exists
    }
    else
    {
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
      Redistribute(sibling_node, node, 0);
      buffer_pool_manager_->UnpinPage(parent_node->ValueAt(1), true); 
      return false;
    }
  }

  if (node_index == (parent_node->GetSize() - 1))        // only has left sibling
  {
    auto sibling_node = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_node->GetSize() - 2)));
    if (node->IsLeafPage() ? 
    ( sibling_node->GetSize() + node->GetSize() < node->GetMaxSize())
    : (sibling_node->GetSize() + node->GetSize() <= node->GetMaxSize()))         // can coalesce
    {
      bool flag = false;
      flag = Coalesce(sibling_node, node, parent_node, node_index, transaction);
      if (!flag)
        buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_node->ValueAt(parent_node->GetSize() - 2), true);
      return true;
    }
    else // can not be coalesced
    {
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
      Redistribute(sibling_node, node, 1);
      buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
      return false;
    }
  }

  // hase both left and right node
  auto sibling_node_left = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index - 1)));  // the left one
  auto sibling_node_right = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index + 1))); // the right one
  if(node->IsLeafPage() ? 
  ((sibling_node_left->GetSize() + node->GetSize()) < node->GetMaxSize()) 
  : ((sibling_node_left->GetSize() + node->GetSize()) <= node->GetMaxSize()))
  {
    // can coalesce with left one
    buffer_pool_manager_->UnpinPage(sibling_node_right->GetPageId(),false); // right one not changed
    bool flag = false;
    flag = Coalesce(sibling_node_left, node, parent_node, node_index, transaction);
    if (!flag)
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);       // modified
    buffer_pool_manager_->UnpinPage(sibling_node_left->GetPageId(), true);  // modified
    return true;                                                            // has been deleted(to left one)
  }
  else if(node->IsLeafPage() ? 
  ((sibling_node_right->GetSize() + node->GetSize()) < node->GetMaxSize()) 
  : ((sibling_node_right->GetSize() + node->GetSize()) <= node->GetMaxSize()))
  {
    // can coalesce with the rigth one
    buffer_pool_manager_->UnpinPage(sibling_node_left->GetPageId(), false);
    bool flag = false;
    flag = Coalesce(node, sibling_node_right, parent_node, node_index + 1, transaction);
    if (!flag)
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);       // modified
    return false;                                                           // has not been deleted
  }
  else
  {
    // can not coalesce, should redistribute
    buffer_pool_manager_->UnpinPage(sibling_node_right->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
    Redistribute(sibling_node_left, node, 1);
    buffer_pool_manager_->UnpinPage(sibling_node_left->GetPageId(), true);
    return false;     // still exists
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) 
{
  node->MoveAllTo(neighbor_node);                      // call the function to move
  buffer_pool_manager_->DeletePage(node->GetPageId()); // delete this leaf page
  parent->Remove(index);                               // update parent
  if (parent->GetSize() >= parent->GetMinSize())       // parent node not deleted
    return false;
  else
    return CoalesceOrRedistribute(parent, transaction);// recursively call
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) 
{
  // except for the parameter of moveallto, all the same as above
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_); 
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);
  if (parent->GetSize() >= parent->GetMinSize())
    return false;
  else
    return CoalesceOrRedistribute(parent, transaction);
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) 
{
  if (index == 1)  // neighbor node left, node right
  {
    // should update parent separating key
    neighbor_node->MoveLastToFrontOf(node);
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage*>(parent_node_page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0)); // new separating key
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true); // modified
  }
  else            // neighbor node right, node left
  {
    // update parent node
    neighbor_node->MoveFirstToEndOf(node);
    auto parent_node_page = buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage*>(parent_node_page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);

  }
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) 
{
  // very similar
  if (index == 1)  //  the same 
  {
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage*>(parent_node_page->GetData());
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
  else
  {
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
    neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 1)        // root size == 1
  {
    if (old_root_node->IsLeafPage())
      return false;                         // size ==1 but leaf page, reasonable
    else                                    // too small, delete
    {
      auto temp_root_node = reinterpret_cast<InternalPage*>(old_root_node);
      root_page_id_ = temp_root_node->RemoveAndReturnOnlyChild();       // get its only child
      auto new_root_node_page = buffer_pool_manager_->FetchPage(root_page_id_);   
      auto new_root_node = reinterpret_cast<BPlusTreePage*>(new_root_node_page->GetData());   // get child node
      new_root_node->SetParentPageId(INVALID_PAGE_ID);                            // delete original root
      buffer_pool_manager_->UnpinPage(root_page_id_, true);                       // modified
      UpdateRootPageId(0);
      return true;        
    }
  }

  return false; // size > 1, do not delete
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  // just find the left most leaf page
  auto temp_res_page = FindLeafPage(nullptr, root_page_id_, true);     // determine the parameter 
  auto leaf_node_page = buffer_pool_manager_->FetchPage(temp_res_page->GetPageId());
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_node_page->GetData());
  buffer_pool_manager_->UnpinPage(temp_res_page->GetPageId(), false);  // not modified
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  // find key, rather than left most
  auto temp_res_page = FindLeafPage(key, root_page_id_, false);
  auto leaf_node_page =  buffer_pool_manager_->FetchPage(temp_res_page->GetPageId());
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_node_page->GetData());
  buffer_pool_manager_->UnpinPage(temp_res_page->GetPageId(), false);
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, leaf_node->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  //If leftMost is true: go as far left as possible (used by Begin()).
  //If leftMost is false: use key to guide traversal.
  auto temp_page = buffer_pool_manager_->FetchPage(page_id);       // start from page_id
  auto temp_node = reinterpret_cast<BPlusTreePage*>(temp_page->GetData());
  temp_page->RLatch();                                             // read latch
  if (temp_node->IsLeafPage())        // if just start from leaf
  {
    // there is no need to find, because it is leaf already
    temp_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    return temp_page;
  }
  else
  {
    auto temp_internal_node = reinterpret_cast<InternalPage*>(temp_node);  // since it is internal, do the translation
    // leftmost scenarios
    page_id_t temp_child_id = (leftMost ? temp_internal_node->ValueAt(0) : temp_internal_node->Lookup(key, processor_));  // has found
    temp_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    return FindLeafPage(key, temp_child_id, leftMost);   // recursively call, until it becomes leaf node
  }
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto header_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record)            // insert_record == 1, add a new record(create a tree)
    header_page->Insert(index_id_, root_page_id_);
  else                          //  == 0, just update Used when the root changes due to a split or coalesce.
    header_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true); // modified
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}


bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
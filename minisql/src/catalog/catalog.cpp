#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //ASSERT(false, "Not Implemented yet");
  //return 0;
  return 4 + 4 + 4 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
    tables_.clear();
    indexes_.clear();
    table_names_.clear();
    index_names_.clear();

    Page* catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* catalog_meta_page_data = catalog_meta_page->GetData();

    if (init) {
        catalog_meta_ = CatalogMeta::NewInstance();
        catalog_meta_->table_meta_pages_.clear();
        catalog_meta_->index_meta_pages_.clear();
        catalog_meta_->SerializeTo(catalog_meta_page_data);
        //next_table_id_ = 0;
        //next_index_id_ = 0;
    }
    else {
        catalog_meta_ = catalog_meta_->DeserializeFrom(catalog_meta_page_data);
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
        for (auto ite : catalog_meta_->table_meta_pages_) {
          table_id_t table_id = ite.first;
          page_id_t page_id = ite.second;
          if (page_id == INVALID_PAGE_ID) {
            continue;
          }
          LoadTable(table_id, page_id);
        }
        for (auto ite: catalog_meta_->index_meta_pages_) {
          index_id_t index_id = ite.first;
          page_id_t page_id = ite.second;
          if (page_id == INVALID_PAGE_ID) {
            continue;
          }
          LoadIndex(index_id, page_id);
        }
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //根据表名，检查要创建的表是否已经存在
  if (table_names_.find(table_name) != table_names_.end()) {
      LOG(ERROR) << "Table '" << table_name << "' already exists.";
      return DB_TABLE_ALREADY_EXIST;
  }
  //生成唯一表ID，提供自增ID的方法
  table_id_t table_id = catalog_meta_->GetNextTableId();

  //分配新页面存储表元数据
  page_id_t table_meta_page_id;
  Page* table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  if (table_meta_page == nullptr) {
    LOG(ERROR) << "Failed to allocate page for table metadata.";
    return DB_FAILED;
  }

  //创建表元数据对象，并序列化到页面
  TableSchema* schema_ = TableSchema::DeepCopySchema(schema);    //deep copy
  page_id_t table_page_id = 0;


  TableHeap* table_heap_ = TableHeap::Create(buffer_pool_manager_, schema_, txn, log_manager_, lock_manager_);
  page_id_t root_page_id = table_heap_->GetFirstPageId();
  TableMetadata* table_meta_ = TableMetadata::Create(table_id, table_name, root_page_id, schema_);

  table_meta_->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);


  table_info = TableInfo::Create();
  table_info->Init(table_meta_, table_heap_);
  table_names_.emplace(table_name, table_id);
  tables_.emplace(table_id, table_info);

  catalog_meta_->table_meta_pages_.emplace(table_id, table_meta_page_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  // 在CreateTable末尾添加：
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //在table_names_的map中查找不到相应的表名
  if (table_names_.find(table_name) == table_names_.end()) {
    LOG(ERROR) << "Table '" << table_name << "' does not exist.";
    return DB_TABLE_NOT_EXIST;
  }

  //根据表名的key找到对应的table_id
  table_id_t table_id = table_names_[table_name];

  //通过table_id找到对应的table_info
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  if (tables.empty()) {
      LOG(INFO) << "GetTables: tables is empty";
      return DB_TABLE_NOT_EXIST;
  }
  //遍历每一张表
  for (auto ite = tables_.begin(); ite != tables_.end(); ite++) {
      tables.push_back(ite->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  //首先检查索引要创建的table_name是否存在
  if (table_names_.find(table_name) == table_names_.end()) {
      LOG(INFO) << "Table " << table_name << " does not exist.";
      return DB_TABLE_NOT_EXIST;
  }

  //检查要创建的index是否已经被创建
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
      LOG(INFO) << "Index " << index_name << " already exists.";
      return DB_INDEX_ALREADY_EXIST;
  }

  //根据table_name找到table_id和table_info
  auto table_id = table_names_[table_name];
  auto table_info = tables_[table_id];

  //创建key_map
  TableSchema* schema = table_info->GetSchema();
  index_id_t index_id;
  // catalog.cpp (CreateIndex函数)
  std::vector<uint32_t> key_map;
  for (const auto &col_name : index_keys) {
    uint32_t col_index;
    if (schema->GetColumnIndex(col_name, col_index) != DB_SUCCESS) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(col_index);  // 保存正确的列索引
  }

  //创建新的index_id
  index_id = catalog_meta_->GetNextIndexId();
  //创建新的index_meta_page
  page_id_t index_meta_page_id;
  Page* index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);

  auto index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_meta->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  dberr_t res = DB_SUCCESS;

  for (auto ite = table_info->GetTableHeap()->Begin(txn); ite != table_info->GetTableHeap()->End() && res == DB_SUCCESS; ite++) {
        Row row;
        ite->GetKeyFromRow(table_info->GetSchema(), index_info->GetIndexKeySchema(), row);
        res = index_info->GetIndex()->InsertEntry(row, ite->GetRowId(), txn);
  }

  if (res != DB_SUCCESS) {
      index_info->GetIndex()->Destroy();
      buffer_pool_manager_->DeletePage(index_meta_page_id);
      delete index_info;
      return res;
  }

  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = index_meta_page_id;

  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.find(table_name) == table_names_.end()) {
      return DB_TABLE_NOT_EXIST;
  }

  auto map_index = index_names_.find(table_name)->second;
  if (map_index.find(index_name) == map_index.end()) {
      return DB_INDEX_NOT_FOUND;
  }

  auto index_id = index_names_.find(table_name)->second.find(index_name)->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if (index_names_.find(table_name) == index_names_.end()) {
      return DB_TABLE_NOT_EXIST;
  }
  auto map_index = index_names_.at(table_name);
  for (auto& ite: map_index) {
      index_id_t index_id = ite.second;
      auto index_info = indexes_.find(index_id)->second;
      indexes.push_back(index_info);
  }
  if (indexes.empty()) {
      return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  TableInfo* table_info = tables_[table_id];

  auto indexes_to_drop = index_names_[table_name];
  for (const auto &entry : indexes_to_drop) {
    IndexInfo *index_info = indexes_[entry.second];
    index_info->GetIndex()->Destroy();
    buffer_pool_manager_->DeletePage(catalog_meta_->
    index_meta_pages_.at(entry.second));
    catalog_meta_->index_meta_pages_.erase(entry.second);
    delete index_info;
    indexes_.erase(entry.second);
  }
  index_names_.erase(table_name);
  buffer_pool_manager_->DeletePage(catalog_meta_->
  table_meta_pages_.at(table_id));
  catalog_meta_->table_meta_pages_.erase(table_id);
  tables_.erase(table_id);
  table_names_.erase(table_name);
  table_info->~TableInfo();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");

  if(table_names_.find(table_name) == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  if(index_names_.find(table_name) == index_names_.end()){
    return DB_INDEX_NOT_FOUND;
    }

  auto map_index = index_names_.find(table_name)->second;
  if(map_index.find(index_name) == map_index.end()) {
    return DB_INDEX_NOT_FOUND;
  }

  auto index_id = map_index.find(index_name)->second;
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);

  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);

  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
    if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
      return DB_SUCCESS;
    }
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");

  if(tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  Page* load_page = buffer_pool_manager_->FetchPage(page_id);

  TableMetadata* table_meta_data;
  TableMetadata::DeserializeFrom(load_page->GetData(),table_meta_data);

  if (table_meta_data == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }

  buffer_pool_manager_->UnpinPage(page_id, false);

  page_id_t root_page_id=table_meta_data->GetFirstPageId();
  auto table_heap = TableHeap::Create(buffer_pool_manager_, root_page_id,table_meta_data->GetSchema(),log_manager_,lock_manager_);

  auto table_info = TableInfo::Create();
  table_info->Init(table_meta_data, table_heap);

  string table_name=table_meta_data->GetTableName();
  page_id_t table_page_id_=table_meta_data->GetFirstPageId();
  TableSchema* schema=table_meta_data->GetSchema();

  tables_[table_id]=table_info;
  table_names_[table_meta_data->GetTableName()]=table_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");

  if(indexes_.find(index_id) != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  Page* load_page = buffer_pool_manager_->FetchPage(page_id);

  IndexMetadata* index_meta_;
  IndexMetadata::DeserializeFrom(load_page->GetData(),index_meta_);

  if (index_meta_ == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);

  IndexInfo* index_info = IndexInfo::Create();
  string index_name=index_meta_->GetIndexName();

  index_id_t table_id=index_meta_->GetTableId();
  TableInfo* table_info=tables_[table_id];

  index_info->Init(index_meta_,table_info,buffer_pool_manager_);
  indexes_[index_id]=index_info;
  index_names_[table_info->GetTableName()][index_name]=index_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.find(table_id) == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}
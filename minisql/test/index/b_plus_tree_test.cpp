#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 2000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0], table_schema);
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1], table_schema);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}


// A new test: DeleteThenReinsertTest

TEST(BPlusTreeTests, DeleteThenReinsertTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);

  // Insert key=42
  GenericKey *key = KP.InitKey();
  std::vector<Field> fields{Field(TypeId::kTypeInt, 42)};
  KP.SerializeFromKey(key, Row(fields), table_schema);
  RowId rid_inserted(999);
  ASSERT_TRUE(tree.Insert(key, rid_inserted));

  // Ensure it exists
  std::vector<RowId> result;
  ASSERT_TRUE(tree.GetValue(key, result));
  ASSERT_EQ(result.back().Get(), rid_inserted.Get());

  // Delete the key
  tree.Remove(key);

  // Ensure it no longer exists
  result.clear();
  ASSERT_FALSE(tree.GetValue(key, result));

  // Reinsert same key with different rid
  RowId rid_reinserted(1234);
  ASSERT_TRUE(tree.Insert(key, rid_reinserted));

  // Check again
  result.clear();
  ASSERT_TRUE(tree.GetValue(key, result));
  ASSERT_EQ(result.back().Get(), rid_reinserted.Get());
}

// A new test: SearchNonExistentKeysTest
TEST(BPlusTreeTests, SearchNonExistentKeysTest) {
  // Initialize database and B+ tree
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);

  // Insert even-numbered keys: 0, 2, 4, ..., 98
  for (int i = 0; i < 100; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    ASSERT_TRUE(tree.Insert(key, RowId(i)));
  }

  // Check that odd-numbered keys do not exist
  for (int i = 1; i < 100; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    std::vector<RowId> result;
    EXPECT_FALSE(tree.GetValue(key, result)) << "Key " << i << " should not be found.";
  }

  // Check edge cases: negative and out-of-range keys
  std::vector<int> test_keys = {-10, -1, 100, 101, 200};
  for (int key_val : test_keys) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, key_val)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    std::vector<RowId> result;
    EXPECT_FALSE(tree.GetValue(key, result)) << "Key " << key_val << " should not be found.";
  }
}

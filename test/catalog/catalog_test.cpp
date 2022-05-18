//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {



// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  DiskManager *disk_manager = new DiskManager("catalog_test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(32, disk_manager);
  Catalog *catalog = new Catalog(bpm, nullptr, nullptr);

  std::vector<std::string> table_names = {"tab1", "tab2", "tab3", "tab4"};
  std::unordered_map<std::string, std::vector<std::string>> index_names;
  index_names[table_names[0]] = {"tab1_index1"};
  index_names[table_names[1]] = {"tab2_index1", "tab2_index2"};
  index_names[table_names[2]] = {"tab3_index1", "tab3_index2", "tab3_index3"};
  index_names[table_names[3]] = {"tab4_index1", "tab4_index2", "tab4_index3", "tab4_index4"};

  // The tables shouldn't exist in the catalog yet.
  for (std::string &table_name : table_names) {
    EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);
  }

  // Put the table & index into the catalog.
  for (std::string &table_name : table_names) {
    std::vector<Column> columns;
    columns.emplace_back("A", TypeId::INTEGER);
    columns.emplace_back("B", TypeId::BOOLEAN);

    Schema schema(columns);
    auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
    (void)table_metadata;

    for (std::string &index_name : index_names[table_name]) {
      Schema key_schema(schema);
      std::vector<uint32_t> key_attrs;
      auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
          nullptr, index_name, table_name, schema, key_schema, key_attrs, 5);
      (void)index_info;
    }
  }

  // Lookup the table & index in the catalog.
  for (size_t i = 0; i < table_names.size(); ++i) {
    TableMetadata *table_metadata = catalog->GetTable(table_names[i]);
    EXPECT_EQ(table_names[i], table_metadata->name_);

    table_metadata = catalog->GetTable(i);
    EXPECT_EQ(table_names[i], table_metadata->name_);
    EXPECT_EQ(i, table_metadata->oid_);

    std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_names[i]);
    EXPECT_EQ(index_names[table_names[i]].size(), indexes.size());
    for (IndexInfo *index_info : indexes) {
      IndexInfo *index_info2 = catalog->GetIndex(index_info->name_, table_names[i]);
      EXPECT_EQ(index_info->name_, index_info2->name_);

      index_info2 = catalog->GetIndex(index_info->index_oid_);
      EXPECT_EQ(index_info->index_oid_, index_info2->index_oid_);
    }
  }

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub

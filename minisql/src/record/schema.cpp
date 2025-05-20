#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs = 0;
  MACH_WRITE_TO(uint32_t, buf + ofs , SCHEMA_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  uint32_t column_count = Schema::GetColumnCount();
  MACH_WRITE_TO(uint32_t , buf + ofs , column_count);
  ofs += sizeof(uint32_t);

  for (const auto &column : columns_) {
    ofs += column->SerializeTo(buf + ofs);
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t initial_size = 2 * sizeof(uint32_t);
  for (const auto &column : Schema::GetColumns()) {
    initial_size += column->GetSerializedSize();
  }
  return initial_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t ofs = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf + ofs);
  ofs += sizeof(uint32_t);
  ASSERT(magic_num == Schema::SCHEMA_MAGIC_NUM , "Invalid magic number");
  uint32_t column_count = MACH_READ_FROM(uint32_t, buf + ofs);
  ofs += sizeof(uint32_t);

  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_count; i++) {
    Column * col = nullptr;
    ofs += Column::DeserializeFrom(buf + ofs, col);
    columns.push_back(col);
  }
  schema = new Schema(columns , true);
  return ofs;
}
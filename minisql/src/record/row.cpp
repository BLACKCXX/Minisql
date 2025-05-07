#include "record/row.h"
#include <stdio.h>
#include <vector>

#include "googletest.h"
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {

  // replace with your code here
  uint32_t ofs = 0;
  uint32_t fields_size = fields_.size();
  MACH_WRITE_TO(uint32_t , buf , fields_size);
  ofs += sizeof(uint32_t);

  // 处理位图 ， use  a bitmap size
  uint32_t bitmap_size = (fields_size+7) / 8; // 我们以八个为一组
  vector<uint32_t> bitmap(bitmap_size , 0);
  // to map which is null
  char temp = 0;
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < bitmap_size; i++) {
    for (uint32_t j = 0; j < 8; j++) {
      char mask = 0;
      temp <<= 1;
      if (cnt < fields_size) {
        mask= fields_[cnt]->IsNull()?1:0;

      }
      temp |= mask;
      cnt++;
    }
    MACH_WRITE_TO(char , buf + ofs , temp);
    temp = 0;
  }
  for (uint32_t i = 0 ; i<fields_size; i++) {
    if (fields_[i]->IsNull() == false) {
      ofs += fields_[i]->SerializeTo(buf + ofs);
    }
  }
  return ofs;

}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {

  // replace with your code here
  uint32_t ofs = 0;
  uint32_t  fields_num = schema->GetColumnCount();
  uint32_t field_size = MACH_READ_FROM(uint32_t , buf + ofs );
  ASSERT(fields_num == field_size, "The total num of buf does not match");

  vector<bool> is_null(field_size, 0);
  char temp ;
  char mask = 1 << 7;
  for (uint32_t i = 0; i < field_size; i++) {
    if (i % 8 == 0) {
      temp = MACH_READ_FROM(char, buf + ofs);
      ofs += sizeof(char);
    }
    is_null[i] = (temp & mask);// 通过与操做全部提取出来
    temp <<= 1;// 从高到低一一提取
  }
  fields_.resize(field_size);
  for (uint32_t i = 0; i < field_size; i++) {
    ofs += fields_[i]->DeserializeFrom(buf + ofs , schema->GetColumn(i)->GetType(),&fields_[i] , is_null[i]);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t ofs = 0;
  uint32_t fields_size = fields_.size();
  ofs += sizeof(uint32_t);
  // then add the number of temps
  ofs += (fields_size + 7) / 8;
  for (uint32_t i = 0; i < fields_size; i++) {
    ofs+= fields_[i]->GetSerializedSize();
  }

  return 0;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

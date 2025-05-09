#include "record/row.h"
#include <stdio.h>
#include <vector>

#include "googletest.h"
/**
 * TODO: Student Implement
 */
/*uint32_t Row::SerializeTo(char *buf, Schema *schema) const {

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

}*/
/*
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t ofs = 0;
  uint32_t fields_size = fields_.size();
  MACH_WRITE_TO(uint32_t, buf + ofs, fields_size);  // 写入字段数量
  ofs += sizeof(uint32_t);

  // 处理空值位图（按字节写入，每个字节包含8个字段的空值状态）
  uint32_t bitmap_size = (fields_size + 7) / 8;
  char *bitmap = buf + ofs;  // 直接操作缓冲区
  memset(bitmap, 0, bitmap_size);  // 初始化为0

  for (uint32_t i = 0; i < fields_size; i++) {
    if (fields_[i]->IsNull()) {
      uint32_t byte_offset = i / 8;
      uint32_t bit_offset = i % 8;
      bitmap[byte_offset] |= (1 << (7 - bit_offset));  // 高位到低位依次填充
    }
  }
  ofs += bitmap_size;

  // 写入非空字段的数据
  for (uint32_t i = 0; i < fields_size; i++) {
    if (!fields_[i]->IsNull()) {
      ofs += fields_[i]->SerializeTo(buf + ofs);
    }
  }
  return ofs;  // 返回实际写入的总字节数
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
    uint32_t fields_count = fields_.size();

    // 1. 存储字段数量本身的大小
    ofs += sizeof(uint32_t);

    // 2. 存储空值位图的大小
    //   每个字段在位图中占1位，向上取整到字节
    ofs += (fields_count + 7) / 8;

    // 3. 累加所有非空字段的实际序列化大小
    for (uint32_t i = 0; i < fields_count; i++) {
      if (!fields_[i]->IsNull()) { // 只计算非空字段的大小
        ofs += fields_[i]->GetSerializedSize();
      }
    }

    return ofs;
  }*/

#include "record/row.h"
#include <iostream>
using namespace std;

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // 若整个row空 则啥也不写进去
  if (fields_.size() == 0) {
    return 0;
  }
  uint32_t ofs = 0;

  // write fields num
  uint32_t fields_num = fields_.size();
  MACH_WRITE_UINT32(buf, fields_num);
  ofs += 4;
  buf += 4;
  // write null bitmap

  uint32_t size = (uint32_t)ceil((double)fields_.size() / 8) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];// use the map to record the
  while (map_num < size / 8) {
    char bitmap = 0;
    for (uint32_t i = 0; i < 8; i++) {

      if ((map_num * 8 + i < fields_.size()) && (fields_.at(map_num * 8 + i)->IsNull() == false)) {
        bitmap = bitmap | (0x01 << (7 - i));
        map[map_num * 8 + i] = 1;
      } else
        map[map_num * 8 + i] = 0;
    }
    map_num++;
    MACH_WRITE_TO(char, buf, bitmap);
    ofs++;
    buf++;
  }
  // write fields_
  for (uint32_t i = 0; i < fields_.size(); i++) {
    // 不为空的field才被写入
    if (map[i]) {
      uint32_t t = fields_.at(i)->SerializeTo(buf);
      buf += t;
      ofs += t;
    }
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  // read fields num
  uint32_t ofs = 0;
  uint32_t field_num = MACH_READ_UINT32(buf);

  buf += 4;
  ofs += 4;
  // read null bitmap
  //注意刚好fields_.size()是8的整数倍的情况
  uint32_t size = (uint32_t)ceil((double)field_num / 8) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];
  while (map_num < size / 8) {
    char bitmap = MACH_READ_FROM(char, buf);
    buf++;
    ofs++;
    for (uint32_t i = 0; i < 8; i++) {
      if (((bitmap >> (7 - i)) & 0x01) != 0) {  // if not null
        map[i + map_num * 8] = 1;
      } else {
        map[i + map_num * 8] = 0;
      }
    }
    map_num++;
  }
  // deserialize
  for (uint32_t i = 0; i < field_num; i++) {
    TypeId type = schema->GetColumn(i)->GetType();
    uint32_t t;
    Field *f;
    if (type == TypeId::kTypeInt) {
      f = new Field(TypeId::kTypeInt, 0);
    } else if (type == TypeId::kTypeChar) {
      f = new Field(TypeId::kTypeChar, const_cast<char *>(""), strlen(const_cast<char *>("")), false);
    } else if (type == TypeId::kTypeFloat) {
      f = new Field(TypeId::kTypeFloat, 0.0f);
    }
    if (map[i] == 0) {
      t = f->DeserializeFrom(buf, type, &f, true); // use bitmap to get the final
      ofs += t;
      buf += t;
    } else {
      t = f->DeserializeFrom(buf, type, &f, false);
      ofs += t;
      buf += t;
    }
    fields_.push_back(f);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here

  // empty row return 0
  if (fields_.size() == 0) {
    return 0;
  }
  uint32_t sum = 0;
  // header size
  sum += 4 + (uint32_t)ceil((double)fields_.size() / 8);
  // fields
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (!fields_.at(i)->IsNull()) {
      sum += fields_.at(i)->GetSerializedSize();
    }
  }
  return sum;
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

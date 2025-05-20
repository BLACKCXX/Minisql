#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  //replace with your code here
  if (buf == nullptr) {
    return 0;
  }

  // write magic number
  uint32_t ofs = 0;
  MACH_WRITE_UINT32(buf + ofs, COLUMN_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  // write column name length
  uint32_t name_len = name_.length() * sizeof(char);
  MACH_WRITE_UINT32(buf + ofs, name_len);
  ofs += sizeof(uint32_t);

  // write column name
  MACH_WRITE_STRING(buf + ofs, name_);
  ofs += name_len;

  // write column type id
  MACH_WRITE_UINT32(buf + ofs, static_cast<uint32_t>(type_));
  ofs += sizeof(uint32_t);

  // write column length
  MACH_WRITE_UINT32(buf + ofs, len_);
  ofs += sizeof(uint32_t);

  // write column index
  MACH_WRITE_UINT32(buf + ofs, table_ind_);
  ofs += sizeof(uint32_t);

  // write nullable flag
  MACH_WRITE_UINT32(buf + ofs, static_cast<uint32_t>(nullable_));
  ofs += sizeof(uint32_t);

  // write unique flag
  MACH_WRITE_UINT32(buf + ofs, static_cast<uint32_t>(unique_));
  ofs += sizeof(uint32_t);

  return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // magic number + column name length + column name + column type id + column length + column index + nullable flag + unique flag
  //sizeof(TypeId) sizeof(bool) cast to sizeof(uint32_t)
  return sizeof(uint32_t) * 7 + name_.length() * sizeof(char);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  if (buf == nullptr) {
    LOG(ERROR) << "buf is nullptr";
    return 0;
  }

  //read magic number
  uint32_t ofs = 0;
  uint32_t MAGIC_NUM = 0;
  MAGIC_NUM = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);

  //read column name length
  uint32_t name_len = 0;
  name_len = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);

  //read column name
  std::string col_name = "";
  char *col_name_buf = new char[name_len];
  memcpy(col_name_buf, buf + ofs, name_len);
  col_name = std::string(col_name_buf);
  ofs += name_len;

  //read column type id
  TypeId col_type;
  col_type = static_cast<TypeId>(MACH_READ_UINT32(buf + ofs));
  ofs += sizeof(uint32_t);

  //read column length
  uint32_t col_len = 0;
  col_len  = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);

  // read column index
  uint32_t col_index = 0;
  col_index = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);

  //read nullable flag
  bool col_nullable = false;
  col_nullable = static_cast<bool>(MACH_READ_UINT32(buf + ofs));
  ofs += sizeof(uint32_t);

  //read unique flag
  bool col_unique = false;
  col_unique = static_cast<bool>(MACH_READ_UINT32(buf + ofs));
  ofs += sizeof(uint32_t);

  if (col_type == TypeId::kTypeChar) {
    column = new Column(col_name, col_type, col_len, col_index, col_nullable, col_unique);
  } else {
    column = new Column(col_name, col_type, col_index, col_nullable, col_unique);
  }
  delete col_name_buf;
  return ofs;
}

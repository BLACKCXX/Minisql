#include "page/bitmap_page.h"

#include "dirent.h"
#include "glog/logging.h"



template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >=  8 * MAX_CHARS)
    return false;// too large
  uint32_t byte_index = next_free_page_ / 8;
  uint32_t bit_index = next_free_page_ % 8;
  bytes[byte_index] |= (1 << bit_index);
  page_offset = next_free_page_;
  for (uint32_t i = 0; i < MAX_CHARS*8; i++) {
    if (((bytes[i / 8] & (1 << (i%8))) == 0)){
      next_free_page_ = i;
      break;
    }

  }
  page_allocated_ += 1;
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= MAX_CHARS*8)
    return false;
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  // 判断该位页是否被分配出去
  if (IsPageFreeLow(byte_index , bit_index)) {
    return false;
  }else {
    bytes[byte_index] &= ~(1 << bit_index);//make it to zero
    next_free_page_ = page_offset;
    page_allocated_ -= 1;
    return true;
  }


}


template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) ? false : true;
}
template <size_t PageSize>
uint32_t BitmapPage<PageSize>::GetNextFreePage(){
  return next_free_page_;
};


template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
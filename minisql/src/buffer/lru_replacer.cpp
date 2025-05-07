#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {

  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  auto iter = frame_map_.find(*frame_id);
  if (iter != frame_map_.end()) {

    frame_map_.erase(iter);

  }

  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {

  auto iter = frame_map_.find(frame_id);
  if (iter != frame_map_.end()) {
    lru_list_.erase(iter->second);
    frame_map_.erase(iter);

  }

}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {

  auto iter = frame_map_.find(frame_id);
  if (iter != frame_map_.end()) {
    return ;
  }
  lru_list_.push_front(frame_id);
  frame_map_[frame_id]= lru_list_.begin();

}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {

  return lru_list_.size();
}
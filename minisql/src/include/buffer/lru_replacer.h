#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include<algorithm>
#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include<unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  list<frame_id_t> lru_list_;  // 双向链表，按访问顺序维护页面
  unordered_map<frame_id_t, list<frame_id_t>::iterator> frame_map_;  // 快速定位页面位置
  mutex mutex_;  // 互斥锁保证线程安全
  size_t max_size_;  // 最大容量
};

#endif  // MINISQL_LRU_REPLACER_H

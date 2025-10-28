#include "../../include/memtable/memtable.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/iterator/iterator.h"
#include "../../include/skiplist/skiplist.h"
#include "../../include/sst/sst.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sys/types.h>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tiny_lsm {

class BlockCache;

// MemTable implementation using PIMPL idiom
MemTable::MemTable() : frozen_bytes(0) {
  current_table = std::make_shared<SkipList>();
}
MemTable::~MemTable() = default;

void MemTable::put_(const std::string &key, const std::string &value,
                    uint64_t tranc_id) {
  // TODO: Lab2.1 无锁版本的 put
  current_table->put(key, value, tranc_id);
  return;
}

void MemTable::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {
  // TODO: Lab2.1 有锁版本的 put
  {
    std::unique_lock<std::shared_mutex> lock(this->cur_mtx);
    this->current_table->put(key, value, tranc_id);
  }
  return;
}

void MemTable::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  // TODO: Lab2.1 有锁版本的 put_batch
  // ? tranc_id 参数可暂时忽略其逻辑判断, 直接插入即可
  {
    std::unique_lock<std::shared_mutex> lock(this->cur_mtx);
    for (const auto &kv : kvs) {
      this->current_table->put(kv.first, kv.second, tranc_id);
    }
  }
  return;
}

SkipListIterator MemTable::cur_get_(const std::string &key, uint64_t tranc_id) {
  // 检查当前活跃的memtable
  // TODO: Lab2.1 从活跃跳表中查询
  if (!this->current_table) { // 活跃表为空
    return SkipListIterator{};
  }
  return this->current_table->get(key, tranc_id);
}

SkipListIterator MemTable::frozen_get_(const std::string &key,
                                       uint64_t tranc_id) {
  // TODO: Lab2.1 从冻结跳表中查询
  // ? 你需要尤其注意跳表的遍历顺序
  // ? tranc_id 参数可暂时忽略, 直接传递参数即可
  for (const auto &sk : this->frozen_tables) {
    auto result = sk->get(key, tranc_id);
    if (result.is_valid()) {
      return result;
    }
  }
  return SkipListIterator{};
}

SkipListIterator MemTable::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab2.1 查询, 建议复用 cur_get_ 和 frozen_get_
  // ? 注意并发控制
  {
    std::shared_lock<std::shared_mutex> lock(cur_mtx);
    auto result = this->cur_get_(key, tranc_id);
    if (result.is_valid())
      return result;
  }
  {
    std::shared_lock<std::shared_mutex> lock(frozen_mtx);
    auto result = this->frozen_get_(key, tranc_id);
    return result;
  }
}

SkipListIterator MemTable::get_(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab2.1 查询, 无锁版本
  return this->current_table->get(key, tranc_id);
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
MemTable::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {

  spdlog::trace("MemTable--get_batch with {} keys", keys.size());
  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
      results;
  results.reserve(keys.size());

  // 1. 先获取活跃表的锁
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  for (size_t idx = 0; idx < keys.size(); idx++) {
    auto key = keys[idx];
    auto cur_res = cur_get_(key, tranc_id);
    if (cur_res.is_valid()) {
      // 值存在且不为空
      results.emplace_back(
          key, std::make_pair(cur_res.get_value(), cur_res.get_tranc_id()));
    } else {
      // 如果活跃表中未找到，先占位
      results.emplace_back(key, std::nullopt);
    }
  }

  // 2. 如果某些键在活跃表中未找到，还需要查找冻结表
  if (!std::any_of(results.begin(), results.end(), [](const auto &result) {
        return !result.second.has_value();
      })) {
    // ! 最后, 需要把 value 为空的键值对标记为 nullopt
    for (auto &[key, value] : results) {
      if (!value.has_value()) {
        value = std::nullopt;
      }
    }
    return results;
  }

  slock1.unlock(); // 释放活跃表的锁
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx); // 获取冻结表的锁
  for (size_t idx = 0; idx < keys.size(); idx++) {
    if (results[idx].second.has_value()) {
      continue; // 如果在活跃表中已经找到，则跳过
    }
    auto key = keys[idx];
    auto frozen_result = frozen_get_(key, tranc_id);
    if (frozen_result.is_valid()) {
      // 值存在且不为空
      results[idx] =
          std::make_pair(key, std::make_pair(frozen_result.get_value(),
                                             frozen_result.get_tranc_id()));
    } else {
      results[idx] = std::make_pair(key, std::nullopt);
    }
  }

  return results;
}

void MemTable::remove_(const std::string &key, uint64_t tranc_id) {
  // TODO Lab2.1 无锁版本的remove
  this->current_table->put(key, "", tranc_id);
  return;
}

void MemTable::remove(const std::string &key, uint64_t tranc_id) {
  // TODO Lab2.1 有锁版本的remove
  {
    std::unique_lock<std::shared_mutex> lock(this->cur_mtx);
    this->current_table->put(key, "", tranc_id);
  }
  return;
}

void MemTable::remove_batch(const std::vector<std::string> &keys,
                            uint64_t tranc_id) {
  // TODO Lab2.1 有锁版本的remove_batch
  {
    std::unique_lock<std::shared_mutex> lock(this->cur_mtx);
    for (const auto &key : keys) {
      this->current_table->put(key, "", tranc_id);
    }
  }
  return;
}

void MemTable::clear() {
  spdlog::info("MemTable--clear(): Clearing all tables");

  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
  frozen_tables.clear();
  current_table->clear();
}

// 将最老的 memtable 写入 SST, 并返回控制类
std::shared_ptr<SST>
MemTable::flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                     std::shared_ptr<BlockCache> block_cache) {
  spdlog::debug("MemTable--flush_last(): Starting to flush memtable to SST{}",
                sst_id);

  // 由于 flush 后需要移除最老的 memtable, 因此需要加写锁
  std::unique_lock<std::shared_mutex> lock(frozen_mtx);

  uint64_t max_tranc_id = 0;
  uint64_t min_tranc_id = UINT64_MAX;

  if (frozen_tables.empty()) {
    // 如果当前表为空，直接返回nullptr
    if (current_table->get_size() == 0) {
      spdlog::debug(
          "MemTable--flush_last(): Current table is empty, returning null");

      return nullptr;
    }
    // 将当前表加入到frozen_tables头部
    frozen_tables.push_front(current_table);
    frozen_bytes += current_table->get_size();
    // 创建新的空表作为当前表
    current_table = std::make_shared<SkipList>();
  }

  // 将最老的 memtable 写入 SST
  std::shared_ptr<SkipList> table = frozen_tables.back();
  frozen_tables.pop_back();
  frozen_bytes -= table->get_size();

  std::vector<std::tuple<std::string, std::string, uint64_t>> flush_data =
      table->flush();
  for (auto &[k, v, t] : flush_data) {
    max_tranc_id = std::max(t, max_tranc_id);
    min_tranc_id = std::min(t, min_tranc_id);
    builder.add(k, v, t);
  }
  auto sst = builder.build(sst_id, sst_path, block_cache);

  spdlog::info("MemTable--flush_last(): SST{} built successfully at '{}'",
               sst_id, sst_path);

  return sst;
}

void MemTable::frozen_cur_table_() {
  // TODO: 冻结活跃表
  frozen_bytes += current_table->get_size();
  frozen_tables.push_front(std::move(this->current_table));
  this->current_table = std::make_shared<SkipList>();
}

void MemTable::frozen_cur_table() {
  // TODO: 冻结活跃表, 有锁版本
  {
    std::shared_lock<std::shared_mutex> lock(this->frozen_mtx);
    frozen_bytes += current_table->get_size();
    frozen_tables.push_front(std::move(this->current_table));
    this->current_table = std::make_shared<SkipList>();
  }
}

size_t MemTable::get_cur_size() {
  std::shared_lock<std::shared_mutex> slock(cur_mtx);
  return current_table->get_size();
}

size_t MemTable::get_frozen_size() {
  std::shared_lock<std::shared_mutex> slock(frozen_mtx);
  return frozen_bytes;
}

size_t MemTable::get_total_size() {
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  return get_frozen_size() + get_cur_size();
}

HeapIterator MemTable::begin(uint64_t tranc_id) {
  // TODO Lab 2.2 MemTable 的迭代器
  // 遍历skiplist构建vector<SearchItem>
  std::vector<SearchItem> vec;
  for (auto it = current_table->begin(); it != current_table->end(); ++it) {
    vec.push_back(SearchItem(it.get_key(), it.get_value(), 0, 0, tranc_id));
  } // 添加活跃表
  int idx = 1;
  for (auto it = this->frozen_tables.begin(); it != frozen_tables.end();
       ++it) { // 添加冷冻表
    for (auto it_ = (*it)->begin(); it_ != (*it)->end(); ++it_) {
      vec.push_back(
          SearchItem(it_.get_key(), it_.get_value(), idx, 0, tranc_id));
    }
    ++idx;
  }
  return HeapIterator(vec, 0);
}

HeapIterator MemTable::end() {
  // TODO Lab 2.2 MemTable 的迭代器
  return HeapIterator{};
}

HeapIterator MemTable::iters_preffix(const std::string &preffix,
                                     uint64_t tranc_id) {

  // TODO Lab 2.3 MemTable 的前缀迭代器
  // preffix: abc str: ab, abe, abcd
  auto res = iters_monotony_predicate(tranc_id, [&](const std::string &str) {
    if (str < preffix)
      return 1;
    if (str.size() >= preffix.size() &&
        str.substr(0, preffix.size()) == preffix) {
      return 0;
    } else {
      return -1;
    }
  });

  if (res.has_value()) {
    return res.value().first;
  }
  return HeapIterator{};
}

std::optional<std::pair<HeapIterator, HeapIterator>>
MemTable::iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO Lab 2.3 MemTable 的谓词查询迭代器起始范围
  // (a1, b1)  (a2, b2) ...
  std::shared_lock<std::shared_mutex> slock1(this->frozen_mtx);
  std::shared_lock<std::shared_mutex> slock2(this->cur_mtx);
  std::vector<SearchItem> item_vec; // SearchItem(std::string k, std::string v,
                                    // int i, int l, uint64_t tranc_id)
  // current_table 的谓词查询结果
  auto result = this->current_table->iters_monotony_predicate(predicate);
  if (result.has_value()) {
    auto [begin, end] = result.value();
    for (auto it = begin; it != end; ++it) {
      item_vec.push_back(SearchItem(it.get_key(), it.get_value(), 0, 0, 0));
    }
  }

  // frozen_tables 的谓词查询结果， 所有的结果项都加入到Heap中
  int table_idx = 1;
  for (auto it_table = this->frozen_tables.begin();
       it_table != this->frozen_tables.end(); ++it_table) {
    auto table = *it_table;
    auto result = table->iters_monotony_predicate(predicate);
    if (result.has_value()) {
      auto [begin, end] = result.value();
      for (auto it = begin; it != end; ++it) {
        item_vec.push_back(
            SearchItem(it.get_key(), it.get_value(), table_idx, 0, 0));
      }
    }
    table_idx++;
  }
  if (item_vec.empty()) {
    return std::nullopt;
  }
  auto head = HeapIterator(item_vec, 0);
  auto tail =
      HeapIterator{}; // auto [begin, end] = iters_monotony_predicate(pred);
                      // for(auto it=begin;!it->is_end();++it) it->is_end():
  return std::make_pair(head, tail);
} // namespace tiny_lsm
} // namespace tiny_lsm
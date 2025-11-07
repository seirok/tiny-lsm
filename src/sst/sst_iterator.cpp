#include "../../include/sst/sst_iterator.h"
#include "../../include/sst/sst.h"
#include "block/block.h"
#include "block/block_iterator.h"
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>

namespace tiny_lsm {

// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.7 实现谓词查询功能
  // step 1: BlockMeta --> Block (by read_block)
  // step 2: Block --> pair of BlockIter
  const auto &meta_entries = sst->meta_entries;
  int l_left = 0;
  int r_left = meta_entries.size() - 1;
  int l_right = 0;
  int r_right = meta_entries.size() - 1;
  bool flag_left = false;
  bool flag_right = false;
  SstIterator left(sst, tranc_id);
  SstIterator right(sst, tranc_id);

  // 寻找左边界
  while (l_left <= r_left) {
    size_t mid = (l_left + r_left) / 2;
    auto block = sst->read_block(mid);
    auto result = block->get_monotony_predicate_iters(tranc_id, predicate);
    if (result.has_value()) {
      const auto &[it_begin, it_end] = result.value();
      if ((*it_begin)->first > block->get_first_key()) {
        left.m_block_idx = mid;
        left.m_block_it = it_begin;
        flag_left = true;
        break;
      } else {
        r_left = mid - 1;
      }
    } else { // 说明该block所存储的key-value范围和predicate的范围没有交集
      int flag = predicate(block->get_first_key());
      if (flag > 0) {
        l_left = mid + 1;
      } else if (flag == 0) {
        throw std::runtime_error("sst_iters_monotony_predicate: logic error");
      } else {
        r_left = mid - 1;
      }
    }
  }

  // 寻找右边界
  while (l_right <= r_right) {
    size_t mid = (l_right + r_right) / 2;
    auto block = sst->read_block(mid);
    auto result = block->get_monotony_predicate_iters(tranc_id, predicate);
    if (result.has_value()) {
      const auto &[it_begin, it_end] = result.value();
      if (*it_end != block->end()) {
        right.m_block_it = it_end;
        right.m_block_idx = mid;
        flag_right = true;
        break;
      } else {
        l_right = mid + 1;
      }
    } else { // 说明该block所存储的key-value范围和predicate的范围没有交集
      int flag = predicate(block->get_first_key());
      if (flag > 0) {
        l_right = mid + 1;
      } else if (flag == 0) {
        throw std::runtime_error("sst_iters_monotony_predicate: logic error");
      } else {
        r_right = mid - 1;
      }
    }
  }

  if (l_left >= meta_entries.size() || r_right < 0) {
    return std::nullopt;
  }

  if (!flag_left) { // 说明某个区间的开头就是整段范围的起点
    auto block = sst->read_block(l_left);
    left.m_block_idx = l_left;
    left.m_block_it = std::make_shared<BlockIterator>(block->begin());
  }

  if (!flag_right) {
    right.m_block_idx = l_right;
    auto block = sst->read_block(l_right);
    if (l_right == meta_entries.size()) {
      right.m_block_it = nullptr; // sst迭代器判空的唯一条件
    } else {
      right.m_block_it = std::make_shared<BlockIterator>(block->begin());
    }
  }

  return std::make_pair(left, right);
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek_first();
  }
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, const std::string &key,
                         uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek(key);
  }
}

void SstIterator::set_block_idx(size_t idx) { m_block_idx = idx; }
void SstIterator::set_block_it(std::shared_ptr<BlockIterator> it) {
  m_block_it = it;
}

void SstIterator::seek_first() {
  // TODO: Lab 3.6 将迭代器定位到第一个key
  auto block = this->m_sst->read_block(0);
  this->m_block_idx = 0;
  this->m_block_it = std::make_shared<BlockIterator>(block->begin());
}

void SstIterator::seek(const std::string &key) {
  // TODO: Lab 3.6 将迭代器定位到指定key的位置
  int idx = this->m_sst->find_block_idx(key);
  if (idx == -1) {
    m_block_it = nullptr;
    m_block_idx = m_sst->num_blocks();
    return;
  }

  m_block_idx = idx;
  auto block = m_sst->read_block(m_block_idx);
  m_block_it = std::make_shared<BlockIterator>(block, key, this->max_tranc_id_);
}

std::string SstIterator::key() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->first;
}

std::string SstIterator::value() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->second;
}

BaseIterator &SstIterator::operator++() {
  // TODO: Lab 3.6 实现迭代器自增
  if (!m_block_it) { // 添加空指针检查
    return *this;
  }
  ++(*m_block_it);
  if (m_block_it->is_end()) {
    m_block_idx++;
    if (m_block_idx < m_sst->num_blocks()) {
      // 读取下一个block
      auto next_block = m_sst->read_block(m_block_idx);
      BlockIterator new_blk_it(next_block, 0, max_tranc_id_);
      (*m_block_it) = new_blk_it;
    } else {
      // 没有下一个block
      m_block_it = nullptr;
    }
  }
  return *this;
}

bool SstIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  if (other.get_type() != IteratorType::SstIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const SstIterator &>(other);
  if (m_sst != other2.m_sst || m_block_idx != other2.m_block_idx) {
    return false;
  }

  if (!m_block_it && !other2.m_block_it) {
    return true;
  }

  if (!m_block_it || !other2.m_block_it) {
    return false;
  }

  return *m_block_it == *other2.m_block_it;
}

bool SstIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  return !(*this == other);
}

SstIterator::value_type SstIterator::operator*() const {
  // TODO: Lab 3.6 实现迭代器解引用
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (**m_block_it);
}

IteratorType SstIterator::get_type() const { return IteratorType::SstIterator; }

uint64_t SstIterator::get_tranc_id() const { return max_tranc_id_; }
bool SstIterator::is_end() const { return !m_block_it; }

bool SstIterator::is_valid() const {
  return m_block_it && !m_block_it->is_end() &&
         m_block_idx < m_sst->num_blocks();
}
SstIterator::pointer SstIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

void SstIterator::update_current() const {
  if (!cached_value && m_block_it && !m_block_it->is_end()) {
    cached_value = *(*m_block_it);
  }
}

std::pair<HeapIterator, HeapIterator>
SstIterator::merge_sst_iterator(std::vector<SstIterator> iter_vec,
                                uint64_t tranc_id) {
  if (iter_vec.empty()) {
    return std::make_pair(HeapIterator(), HeapIterator());
  }

  HeapIterator it_begin;
  for (auto &iter : iter_vec) {
    while (iter.is_valid() && !iter.is_end()) {
      it_begin.items.emplace(
          iter.key(), iter.value(), -iter.m_sst->get_sst_id(), 0,
          tranc_id); // ! 此处的level暂时没有作用, 都作用于同一层的比较
      ++iter;
    }
  }
  return std::make_pair(it_begin, HeapIterator());
}
} // namespace tiny_lsm
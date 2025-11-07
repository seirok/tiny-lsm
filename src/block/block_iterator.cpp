#include "../../include/block/block_iterator.h"
#include "../../include/block/block.h"
#include <cstdint>
#include <memory>
#include <stdexcept>

class Block;

namespace tiny_lsm {
BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t index,
                             uint64_t tranc_id)
    : block(b), current_index(index), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

/**
 * @brief 块迭代器构造函数
 *
 * 该构造函数创建一个指向指定键位置的块迭代器。通过二分查找在块中定位
 * 给定键的位置，如果找到则定位到该键，否则将迭代器设置为结束位置。
 *
 * @param b 指向目标块的共享指针
 * @param key 要查找的目标键
 * @param tranc_id 事务ID，用于可见性判断
 *
 * @note 如果键存在于块中，迭代器将定位到该键对应的位置
 * @note 如果键不存在，迭代器将被设置为结束位置（等同于end()迭代器）
 * @note 使用二分查找算法优化查找效率
 * @note 结束迭代器通过将current_index设置为offsets的大小来构造
 *
 * @see Block::get_idx_binary()
 * @see Block::end()
 */

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  // TODO: Lab3.2 创建迭代器时直接移动到指定的key位置
  // ? 你需要借助之前实现的 Block 类的成员函数
  auto idx = block->get_idx_binary(key, tranc_id); // idx是offsets中的索引
  if (idx.has_value()) {
    this->current_index = idx.value();
  } else {
    this->current_index =
        this->block->offsets
            .size(); // 构造成end迭代器（因为end()函数用""来构造）
  }
}

// BlockIterator::BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id)
//     : block(b), current_index(0), tranc_id_(tranc_id),
//       cached_value(std::nullopt) {
//   skip_by_tranc_id();
// }

BlockIterator::pointer BlockIterator::operator->() const {
  // TODO: Lab3.2 -> 重载
  this->cached_value = this->operator*();
  if (this->cached_value.has_value()) {
    return &(this->cached_value.value());
  }
  return nullptr;
}

BlockIterator &BlockIterator::operator++() {
  // TODO: Lab3.2 ++ 重载
  // ? 在后续的Lab实现事务后，你可能需要对这个函数进行返修
  this->current_index++;
  return *this;
}

BlockIterator &BlockIterator::operator--() {
  // TODO: Lab3.2 自己增加的实现，主要为了Lab3.3
  if (this->current_index == 0) {
    throw std::runtime_error("BlockIterator::operator--: Logic error");
  }
  this->current_index--;
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  // TODO: Lab3.2 == 重载
  if (this->current_index == other.current_index) {
    return true;
  }
  return false;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  // TODO: Lab3.2 != 重载
  if (this->current_index != other.current_index) {
    return true;
  }
  return false;
}

BlockIterator::value_type BlockIterator::operator*() const {
  // TODO: Lab3.2 * 重载
  size_t offset = this->block->offsets[this->current_index];
  auto entry = this->block->get_entry_at(offset);
  return {entry.key, entry.value};
}

// 可以通过这个确认current_index就是offsets中的索引
bool BlockIterator::is_end() { return current_index == block->offsets.size(); }

void BlockIterator::update_current() const {
  // TODO: Lab3.2 更新当前指针
  // ? 该函数是可选的实现, 你可以采用自己的其他方案实现->, 而不是使用
  // ? cached_value 来缓存当前指针
}

void BlockIterator::skip_by_tranc_id() {
  // TODO: Lab3.2 * 跳过事务ID
  // ? 只是进行标记以供你在后续Lab实现事务功能后修改
  // ? 现在你不需要考虑这个函数
}
} // namespace tiny_lsm
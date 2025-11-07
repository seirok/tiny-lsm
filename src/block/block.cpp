#include "../../include/block/block.h"
#include "../../include/block/block_iterator.h"
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>

namespace tiny_lsm {
Block::Block(size_t capacity) : capacity(capacity) {}

std::vector<uint8_t> Block::encode() {
  // TODO Lab 3.1 编码单个类实例形成一段字节数组
  // 复制data部分
  std::vector<uint8_t> vec(this->data.begin(), this->data.end());
  // offsets的字节数组
  size_t entry_num = this->offsets.size();
  for (size_t i = 0; i < entry_num; i++) {
    const uint8_t *p = reinterpret_cast<const uint8_t *>(&this->offsets[i]);
    vec.push_back(p[0]);
    vec.push_back(p[1]);
  }
  const uint8_t *pe = reinterpret_cast<const uint8_t *>(&entry_num);
  vec.push_back(pe[0]);
  vec.push_back(pe[1]);

  return vec;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t> &encoded,
                                     bool with_hash) {
  // TODO Lab 3.1 解码字节数组形成类实例
  // 填充data 和 offsets

  // static, 创建空的Block对象
  std::shared_ptr<Block> block = std::make_shared<Block>();

  // Entry 数量
  if (encoded.size() < 2) {
    throw std::runtime_error("Block::decode:Logic Error: encoded.size() < 2");
  }
  uint16_t encoded_size = encoded.size();
  auto high = encoded[encoded_size - 1];
  auto low = encoded[encoded_size - 2];
  uint16_t entry_num = low + (high << 8);

  // 提取data部分
  auto end = encoded_size - entry_num * 2 - 2;
  if (end < 12) {
    throw std::runtime_error("Block::decode:Logic Error: end < 12");
  }
  block->data = std::vector<uint8_t>(encoded.begin(), encoded.begin() + end);

  // 填充offsets
  uint16_t cur_byte = 0; // 指向当前的字节(uint_8),即encoded/data的索引
  for (uint16_t i = 0; i < entry_num; i++) {
    block->offsets.push_back(cur_byte);
    auto key_len =
        encoded[cur_byte] +
        (encoded[cur_byte + 1] << 8); // 有问题，你并不知道这是大端序还是小端序
    cur_byte += (key_len + 2); // 现在指向val_len
    auto val_len = encoded[cur_byte] + (encoded[cur_byte + 1] << 8);
    cur_byte +=
        (val_len + 2 +
         8); // 现在指向下一个key_len，即下一个entry的偏移；8 位是给tranc_id的
  }

  return block;
}

std::string Block::get_first_key() {
  if (data.empty() || offsets.empty()) {
    return "";
  }

  // 读取第一个key的长度（前2字节）
  uint16_t key_len;
  memcpy(&key_len, data.data(), sizeof(uint16_t));

  // 读取key
  std::string key(reinterpret_cast<char *>(data.data() + sizeof(uint16_t)),
                  key_len);
  return key;
}

size_t Block::get_offset_at(size_t idx) const {
  if (idx > offsets.size()) {
    throw std::runtime_error("idx out of offsets range");
  }
  return offsets[idx];
}

bool Block::add_entry(const std::string &key, const std::string &value,
                      uint64_t tranc_id, bool force_write) {
  // TODO Lab 3.1 添加一个键值对到block中
  // ? 返回值说明：
  // ? true: 成功添加
  // ? false: block已满, 拒绝此次添加
  auto key_len = static_cast<uint16_t>(key.size()); // 有截断风险
  auto val_len = static_cast<uint16_t>(value.size());
  // 计算总长度
  size_t prepare_to_add_bytes_num =
      sizeof(uint16_t) * 2 + 8 * sizeof(uint8_t) + key_len * sizeof(uint8_t) +
      val_len * sizeof(uint8_t) + sizeof(uint16_t);
  if (prepare_to_add_bytes_num + this->cur_size() > this->capacity) {
    return false;
  }
  uint8_t k1;
  uint8_t k2;
  const uint8_t *p = reinterpret_cast<const uint8_t *>(&key_len);
  k1 = p[0]; // 低地址字节
  k2 = p[1]; // 高地址字节
  this->data.push_back(k1);
  this->offsets.push_back(this->data.size() - 1);
  this->data.push_back(k2);
  std::vector<uint8_t> key_vec(key.begin(), key.end());
  this->data.insert(this->data.end(), key_vec.begin(), key_vec.end());
  uint8_t v1;
  uint8_t v2;
  p = reinterpret_cast<const uint8_t *>(&val_len);
  v1 = p[0];
  v2 = p[1];
  this->data.push_back(v1);
  this->data.push_back(v2);
  std::vector<uint8_t> val_vec(value.begin(), value.end());
  this->data.insert(this->data.end(), val_vec.begin(), val_vec.end());
  p = reinterpret_cast<const uint8_t *>(&tranc_id);
  for (size_t i = 0; i < (sizeof(uint64_t) / sizeof(uint8_t)); i++) {
    this->data.push_back(p[i]);
  }

  return true;
}

// 从指定偏移量获取entry的key
std::string Block::get_key_at(size_t offset) const {
  // TODO Lab 3.1 从指定偏移量获取entry的key
  auto key_len = this->data[offset] + (this->data[offset + 1] << 8);
  std::string res(this->data.begin() + offset + 2,
                  this->data.begin() + offset + 2 + key_len);
  if (res == "") {
    std::cout << "Block::get_key_at: Empty String" << std::endl;
  }
  return res;
}

// 从指定偏移量获取entry的value
std::string Block::get_value_at(size_t offset) const {
  // TODO Lab 3.1 从指定偏移量获取entry的value
  // offset 是在 data 中的 key 的偏移
  uint16_t key_len;
  memcpy(&key_len, this->data.data() + offset, sizeof(uint16_t));

  uint16_t val_len;
  memcpy(&val_len, this->data.data() + offset + 2 + key_len, sizeof(uint16_t));
  auto res =
      std::string(this->data.begin() + offset + 2 + key_len + 2,
                  this->data.begin() + offset + 2 + key_len + 2 + val_len);

  return res;
}

uint64_t Block::get_tranc_id_at(size_t offset) const {
  // TODO Lab 3.1 从指定偏移量获取entry的tranc_id
  // ? 你不需要理解tranc_id的具体含义, 直接返回即可
  // 注意！！！ 这里的offset 是紧靠着tranc_id 8字节后的下一个entry的起始地址
  uint64_t tranc_id;
  memcpy(&tranc_id, this->data.data() + offset - 8, sizeof(uint64_t));
  return tranc_id;
}

// 比较指定偏移量处的key与目标key
int Block::compare_key_at(size_t offset, const std::string &target) const {
  std::string key = get_key_at(offset);
  return key.compare(target);
}

// 相同的key连续分布, 且相同的key的事务id从大到小排布
// 这里的逻辑是找到最接近 tranc_id 的键值对的索引位置
int Block::adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id) {
  // TODO Lab3.1 不需要在Lab3.1中实现, 只是进行标记,
  // ? 后续实现事务后需要更新这里的实现
  return -1;
}

bool Block::is_same_key(size_t idx, const std::string &target_key) const {
  if (idx >= offsets.size()) {
    return false; // 索引超出范围
  }
  return get_key_at(offsets[idx]) == target_key;
}

// 使用二分查找获取value
// 要求在插入数据时有序插入
std::optional<std::string> Block::get_value_binary(const std::string &key,
                                                   uint64_t tranc_id) {
  auto idx = get_idx_binary(key, tranc_id);
  if (!idx.has_value()) {
    return std::nullopt;
  }

  return get_value_at(offsets[*idx]); // optional 对 * 有重载
}

std::optional<size_t> Block::get_idx_binary(const std::string &key,
                                            uint64_t tranc_id) {
  // TODO Lab 3.1 使用二分查找获取entry起始的索引offsets[idx], 返回在 offsets
  // 下的下标idx
  int left = 0;
  int right = this->offsets.size() - 1;
  while (left <= right) {
    size_t mid = (left + right) / 2;
    auto offset = this->offsets[mid]; // data 里的offset
    auto str = get_key_at(offset);
    uint64_t t_id;
    if (mid == this->offsets.size() - 1) {
      t_id = get_tranc_id_at(this->data.size());
    } else {
      t_id = get_tranc_id_at(this->offsets[mid + 1]);
    }

    if (str < key) { // str=="ap"  key==
      left = mid + 1;
    } else if (str == key) {
      if (tranc_id == 0) { // tranc_id为0，说明其要查找对应最新的key-value
        if (mid == 0) { // 最左侧的元素
          return mid;
        }
        size_t k = 1;
        while (true) {
          auto k_str = get_key_at(this->offsets[mid - k]);
          if (k_str < key) {
            return mid - k + 1;
          } else if (k_str == key) {
            if (mid == k)
              return 0;
          } else {
            std::cout << "Block::get_idx_binary: Logic error" << std::endl;
          }
          k++;
        }
      }
      if (t_id < tranc_id) { // t_id==1, tranc_id==3     orange3(^)   orange1
        right = mid - 1;
        continue;
      } else if (t_id > tranc_id) {
        left = mid + 1;
        continue;
      } else {
        return mid;
      }
    } else {
      right = mid - 1;
    }
  }
  return std::nullopt;
}

std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t tranc_id, const std::string &preffix) {
  // TODO Lab 3.3 获取前缀匹配的区间迭代器
  std::function<int(const std::string &)> predicate(
      [&](const std::string &str) {
        if (str.substr(0, preffix.size()) == preffix) {
          return 0;
        } else if (str < preffix) { // preffix=="abc" str: ab, abe,
          return 1;
        } else {
          return -1;
        }
      });
  return get_monotony_predicate_iters(tranc_id, predicate);
}

// 返回第一个满足谓词的位置和最后一个满足谓词的位置
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// 返回的区间是闭区间, 开区间需要手动对返回值自增
// predicate返回值:
//   0: 满足谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.3 使用二分查找获取满足谓词的区间迭代器
  int entry_num = this->offsets.size();
  int l_left = 0, l_right = 0;
  int r_left = entry_num - 1, r_right = entry_num - 1;
  int left = -1;
  int right = -1;
  while (l_left <= r_left) {
    int mid_left = (l_left + r_left) / 2;
    auto offset_left = this->offsets[mid_left];
    // offset --> key
    const auto &key_left = this->get_key_at(offset_left);
    int flag_left = predicate(key_left);
    if (flag_left <= 0) { // key in range
      r_left = mid_left - 1;
    } else {
      l_left = mid_left + 1;
    }
  }
  left = l_left;

  while (l_right <= r_right) {
    int mid_right = (l_right + r_right) / 2;
    auto offset_right = this->offsets[mid_right];
    // offset --> key
    const auto &key_right = this->get_key_at(offset_right);
    int flag_right = predicate(key_right);
    if (flag_right >= 0) { // key in range
      l_right = mid_right + 1;
    } else {
      r_right = mid_right - 1;
    }
  }
  right = l_right;

  if (left >= entry_num || right <= 0) {
    return std::nullopt;
  }
  return std::make_pair<std::shared_ptr<BlockIterator>,
                        std::shared_ptr<BlockIterator>>(
      std::make_shared<BlockIterator>(
          shared_from_this(), this->get_key_at(this->get_offset_at(left)),
          tranc_id),
      std::make_shared<BlockIterator>(
          shared_from_this(), this->get_key_at(this->get_offset_at(right)),
          tranc_id));
}

Block::Entry Block::get_entry_at(size_t offset) const {
  Entry entry;
  entry.key = get_key_at(offset);
  entry.value = get_value_at(offset);
  entry.tranc_id = get_tranc_id_at(offset);
  return entry;
}

size_t Block::size() const { return offsets.size(); }

size_t Block::cur_size() const {
  return data.size() + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

bool Block::is_empty() const { return offsets.empty(); }

BlockIterator Block::begin(uint64_t tranc_id) {
  // TODO Lab 3.2 获取begin迭代器
  return BlockIterator(shared_from_this(), get_first_key(), tranc_id);
}

BlockIterator Block::end() {
  // TODO Lab 3.2 获取end迭代器
  return BlockIterator(shared_from_this(), "", 0);
}
} // namespace tiny_lsm
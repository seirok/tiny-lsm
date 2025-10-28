#include "../../include/block/blockmeta.h"
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <sys/types.h>

namespace tiny_lsm {
BlockMeta::BlockMeta() : offset(0), first_key(""), last_key("") {}

BlockMeta::BlockMeta(size_t offset, const std::string &first_key,
                     const std::string &last_key)
    : offset(offset), first_key(first_key), last_key(last_key) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     std::vector<uint8_t> &metadata) {
  // TODO: Lab 3.4 将内存中所有`Blcok`的元数据编码为二进制字节数组
  // ? 输入输出都由参数中的引用给定, 你不需要自己创建`vector`
  // 对于每一个block meta， 构建对应的meta entry
  // MetaEntry 结构如下:
  // | offset (32) | first_key_len (16) | first_key (first_key_len) |
  // last_key_len(16) | last_key (last_key_len) |
  // 最后，在metadata 首尾分别加上元素数量(32)以及哈希值(32)

  // entry num
  uint32_t entries_num = static_cast<uint32_t>(meta_entries.size());
  auto p_num = reinterpret_cast<const uint8_t *>(&entries_num);
  for (int i = 0; i < sizeof(uint32_t) / sizeof(uint8_t); i++) {
    metadata.push_back(p_num[i]);
  }
  for (const auto &meta_entry : meta_entries) {
    // 填充 offset (32)
    uint32_t offset = static_cast<uint32_t>(meta_entry.offset);
    auto p_off = reinterpret_cast<const uint8_t *>(&offset);
    for (int i = 0; i < (sizeof(uint32_t) / sizeof(uint8_t)); i++) {
      metadata.push_back(p_off[i]);
    }

    // uint16_t -> uint8_t;  填充first_key_len (16)
    uint16_t first_key_len = static_cast<uint16_t>(meta_entry.first_key.size());
    auto p_first = reinterpret_cast<const uint8_t *>(&first_key_len);
    for (int i = 0; i < (sizeof(uint16_t) / sizeof(uint8_t)); i++) {
      metadata.push_back(p_first[i]);
    }

    // string -> vector<uint8_t>
    const auto &vec = std::vector<uint8_t>(meta_entry.first_key.begin(),
                                           meta_entry.first_key.end());
    metadata.insert(metadata.end(), vec.begin(), vec.end());

    // size_t -> uint8_t;  填充last_key_len (16)
    uint16_t last_key_len = static_cast<uint16_t>(meta_entry.last_key.size());
    auto p_last = reinterpret_cast<const uint8_t *>(&last_key_len);
    for (int i = 0; i < (sizeof(uint16_t) / sizeof(uint8_t)); i++) {
      metadata.push_back(p_last[i]);
    }

    // string -> vector<uint8_t>
    const auto &value = std::vector<uint8_t>(meta_entry.last_key.begin(),
                                             meta_entry.last_key.end());
    metadata.insert(metadata.end(), value.begin(), value.end());
  }
  std::hash<std::string> hash_fn;
  size_t hash_val = hash_fn(std::string(metadata.begin() + 4, metadata.end()));
  uint32_t hash_val_32 = static_cast<uint32_t>(hash_val);
  auto p_hash = reinterpret_cast<const uint8_t *>(&hash_val_32);
  for (int i = 0; i < sizeof(uint32_t) / sizeof(uint8_t); i++) {
    metadata.push_back(p_hash[i]);
  }
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  // TODO: Lab 3.4 将二进制字节数组解码为内存中的`Blcok`元数据
  if (metadata.size() < 8) {
    throw std::runtime_error("BlockMeta::decode_meta_from_slice: Logic Error");
  }
  std::vector<BlockMeta> vec_meta;
  size_t cur = 4;                     // 前4个字节存放着entries_num
  while (cur < metadata.size() - 4) { // 后4个字节存放了哈希值
    BlockMeta meta;

    // decode offset
    uint32_t offset;
    auto p_off = reinterpret_cast<uint8_t *>(&offset);
    for (int i = 0; i < sizeof(uint32_t) / sizeof(uint8_t); i++) {
      p_off[i] = metadata[cur++];
    }
    meta.offset = offset;

    // decode first_key_len
    uint16_t first_key_len;
    auto p = reinterpret_cast<uint8_t *>(&first_key_len);
    p[0] = metadata[cur++];
    p[1] = metadata[cur++];
    std::string str(metadata.begin() + cur,
                    metadata.begin() + cur + first_key_len);
    meta.first_key = str;
    cur += first_key_len; // 此时指向last_key_len开头

    // last_key_len
    uint16_t last_key_len;
    auto pl = reinterpret_cast<uint8_t *>(&last_key_len);
    pl[0] = metadata[cur++];
    pl[1] = metadata[cur++];

    // last_key
    std::string str_last(metadata.begin() + cur,
                         metadata.begin() + cur + last_key_len);
    meta.last_key = str_last;
    cur += last_key_len;
    vec_meta.push_back(meta);
  }

  // decode hash
  uint32_t hash_val_ori;
  auto p_hash = reinterpret_cast<uint8_t *>(&hash_val_ori);
  for (int i = 0; i < sizeof(uint32_t) / sizeof(uint8_t); i++) {
    p_hash[i] = metadata[cur++];
  }

  // 检验Hash值
  std::hash<std::string> hash_fcn;
  uint32_t hash_val = static_cast<uint32_t>(
      hash_fcn(std::string(metadata.begin() + 4, metadata.end() - 4)));
  if (hash_val != hash_val_ori) {
    throw std::runtime_error(
        "BlockMeta::decode_meta_from_slice: Hash value change");
  }

  return vec_meta;
}
} // namespace tiny_lsm
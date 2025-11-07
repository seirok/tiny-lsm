#include "../../include/sst/sst.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/sst/sst_iterator.h"
#include "block/block.h"
#include "block/blockmeta.h"
#include <algorithm>
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>

namespace tiny_lsm {

// **************************************************
// SST
// **************************************************

std::shared_ptr<SST> SST::open(size_t sst_id, FileObj file,
                               std::shared_ptr<BlockCache> block_cache) {
  // TODO Lab 3.6 打开一个SST文件, 返回一个描述类
  // SST最后4个字节用来存放Meta Section 的偏移
  // sst 文件结构：
  // | Block 1 | Block 2 |...| Block 5 | meta-section | meta-offset |

  auto sst = std::make_shared<SST>();

  // 从二进制数据中读取meta offset
  uint32_t meta_offset;
  const auto &vec_offset =
      file.read_to_slice(file.size() - sizeof(uint32_t), sizeof(uint32_t));
  auto p = reinterpret_cast<uint8_t *>(&meta_offset);
  for (int i = 0; i < vec_offset.size(); i++) {
    p[i] = vec_offset[i];
  }

  // 读取meta_entries
  const auto &meta_entries_data = file.read_to_slice(
      meta_offset, file.size() - sizeof(uint32_t) - meta_offset);
  sst->meta_entries = BlockMeta::decode_meta_from_slice(meta_entries_data);

  // 获取sst的first_key 和 last_key
  const auto &sst_first_key = sst->meta_entries[0].first_key;
  const auto &sst_last_key = sst->meta_entries.back().last_key;

  // 基本信息
  sst->sst_id = sst_id;
  sst->file = std::move(file);
  sst->meta_block_offset = meta_offset;
  sst->first_key = sst_first_key;
  sst->last_key = sst_last_key;
  return sst;
}

void SST::del_sst() { file.del_file(); }

std::shared_ptr<SST> SST::create_sst_with_meta_only(
    size_t sst_id, size_t file_size, const std::string &first_key,
    const std::string &last_key, std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<SST>();
  sst->file.set_size(file_size);
  sst->sst_id = sst_id;
  sst->first_key = first_key;
  sst->last_key = last_key;
  sst->meta_block_offset = 0;
  sst->block_cache = block_cache;

  return sst;
}

std::shared_ptr<Block> SST::read_block(size_t block_idx) {
  // TODO: Lab 3.6 根据 block 的 id 读取一个 `Block`
  // SST类只是对SST文件的描述类，并不储存实际数据
  // 读取offset
  const auto &meta_entry = this->meta_entries[block_idx];
  size_t offset = meta_entry.offset;

  // 计算读取长度
  size_t length;
  if (block_idx == this->meta_entries.size() - 1) { // 最后一个block
    length = meta_block_offset - offset;
  } else {
    size_t next_block_offset = this->meta_entries[block_idx + 1].offset;
    length = next_block_offset - offset;
  }

  // 从sst文件中读取block的二进制数据
  auto data = this->file.read_to_slice(offset, length);

  // 解码为Block对象
  auto block_read = Block::decode(data);
  return block_read;
}

int SST::find_block_idx(const std::string &key) {
  // 先在布隆过滤器判断key是否存在
  // TODO: Lab 3.6 二分查找
  // ? 给定一个 `key`, 返回其所属的 `block` 的索引
  // ? 如果没有找到包含该 `key` 的 Block，返回-1
  int left = 0;
  int right = this->meta_entries.size() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    const auto &block = this->meta_entries[mid];
    if (key >= block.first_key && key <= block.last_key) {
      return mid;
    } else if (left == right) {
      return -1;
    } else if (key < block.first_key) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return -1;
}

SstIterator SST::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 3.6 根据查询`key`返回一个迭代器
  // ? 如果`key`不存在, 返回一个无效的迭代器即可
  auto self = shared_from_this();
  auto sst_it = SstIterator(self, tranc_id);
  if (key < this->first_key || key > this->last_key) {
    return this->end();
  }
  return SstIterator(shared_from_this(), key, tranc_id);
}

size_t SST::num_blocks() const { return meta_entries.size(); }

std::string SST::get_first_key() const { return first_key; }

std::string SST::get_last_key() const { return last_key; }

size_t SST::sst_size() const { return file.size(); }

size_t SST::get_sst_id() const { return sst_id; }

SstIterator SST::begin(uint64_t tranc_id) {
  // TODO: Lab 3.6 返回起始位置迭代器
  throw std::runtime_error("Not implemented");
}

SstIterator SST::end() {
  // TODO: Lab 3.6 返回终止位置迭代器
  auto sst_it = SstIterator(shared_from_this(), 0);
  sst_it.m_block_it = nullptr;
  return sst_it;
}

std::pair<uint64_t, uint64_t> SST::get_tranc_id_range() const {
  return std::make_pair(min_tranc_id_, max_tranc_id_);
}

// **************************************************
// SSTBuilder
// **************************************************

SSTBuilder::SSTBuilder(size_t block_size, bool has_bloom) : block(block_size) {
  // 初始化第一个block
  if (has_bloom) {
    bloom_filter = std::make_shared<BloomFilter>(
        TomlConfig::getInstance().getBloomFilterExpectedSize(),
        TomlConfig::getInstance().getBloomFilterExpectedErrorRate());
  }
  meta_entries.clear();
  data.clear();
  first_key.clear();
  last_key.clear();
}

void SSTBuilder::add(const std::string &key, const std::string &value,
                     uint64_t tranc_id) {
  // TODO: Lab 3.5 添加键值对
  // 本身是管理若干键值对的容器，而键值对则来源于下一层的Memtable
  // !!! 以下代码正确的前提是SST是通过有序遍历Memtable得到的

  if (!this->block.size()) { // block的offsets数组为空
    this->first_key = key;
  }
  if (this->block.add_entry(key, value, tranc_id, true)) {
    this->last_key = key;
    return;
  }
  finish_block();
  if (!this->block.add_entry(key, value, tranc_id, true)) {
    throw std::runtime_error(
        "Block size is too small that couldn't store singel key-value.");
  }
  this->last_key = key;
  return;
}

size_t SSTBuilder::estimated_size() const { return data.size(); }

/**
 * @brief 完成当前块的构建并将其编码到数据缓冲区
 *
 * 该函数在检测到当前块容量超出阈值时被调用，负责将当前块的数据进行编码，
 * 生成块元数据信息，并将编码后的块数据添加到总数据缓冲区中。
 *
 * @note 调用此函数后会清空当前块，为下一个块的构建做准备
 * @note 会记录块的起始偏移量、第一个键和最后一个键作为元数据
 * @note 编码后的块数据会被追加到数据缓冲区的末尾
 * @note 使用移动语义来清空当前块，避免不必要的拷贝
 *
 * @see Block::encode()
 * @see BlockMeta
 */
void SSTBuilder::finish_block() {
  // TODO: Lab 3.5 构建块
  // ? 当 add
  // 函数发现当前的`block`容量超出阈值时，需要将其编码到`data`，并清空`block`
  const auto &first_key = this->block.get_first_key();
  auto old_block = std::move(this->block); // 通过调用移动构造函数，来清空block
  auto encoded_data = old_block.encode();

  // 添加block所对应的meta entry
  auto meta_entry = BlockMeta(this->data.size(), first_key, this->last_key);
  this->meta_entries.push_back(std::move(meta_entry));

  // 加入到二进制数据中
  this->data.insert(this->data.end(), encoded_data.begin(), encoded_data.end());
}

/**
 * @brief 构建SST文件并写入磁盘
 *
 * 该函数完成SST文件的最终构建，包括处理最后一个数据块、生成元数据段、
 * 添加元数据偏移量，并将完整内容写入磁盘文件。最终返回指向新构建SST对象的共享指针。
 *
 * @param sst_id SST文件的唯一标识符
 * @param path SST文件的存储路径
 * @param block_cache 块缓存，用于SST对象的缓存管理
 * @return std::shared_ptr<SST> 指向新构建SST对象的共享指针
 *
 * @throw std::runtime_error 如果SST构建器为空（没有数据）
 *
 * @note 文件格式：数据块1 + 数据块2 + ... + 元数据段 + 元数据偏移量(4字节)
 * @note 会先完成最后一个块的构建，然后生成元数据段
 * @note 元数据偏移量以小端字节序存储在文件末尾
 * @note 构建完成后会创建对应的SST描述对象并返回
 *
 * @see SSTBuilder::finish_block()
 * @see BlockMeta::encode_meta_to_slice()
 * @see FileObj::create_and_write()
 */
std::shared_ptr<SST>
SSTBuilder::build(size_t sst_id, const std::string &path,
                  std::shared_ptr<BlockCache> block_cache) {
  // TODO 3.5 构建一个SST
  // SST = block1 + block2 + ... + meta section + meta-offset(4 bytes)
  // 构建完之后将SST写入文件里，该文件对象FileObj由SST类管理

  auto sst = std::make_shared<SST>();
  if (this->block.is_empty() && !this->meta_entries.size()) {
    throw std::runtime_error("SSTBuilder::build: Empty SST");
  }
  this->finish_block(); //结束当前块

  // blocks + meta info
  std::vector<uint8_t> meta_section;
  BlockMeta::encode_meta_to_slice(this->meta_entries, meta_section);

  std::vector<uint8_t> file_content = std::move(this->data);

  sst->meta_block_offset = file_content.size();

  file_content.insert(file_content.end(), meta_section.begin(),
                      meta_section.end());

  // 再加上4字节的meta-offset信息
  auto p_off = reinterpret_cast<const uint8_t *>(&sst->meta_block_offset);
  std::vector<uint8_t> offset_vec(sizeof(uint32_t));
  for (int i = 0; i < offset_vec.size(); i++) {
    offset_vec[i] = p_off[i];
  }
  file_content.insert(file_content.end(), offset_vec.begin(), offset_vec.end());

  // 此时file_content: block1 + block2 + meta section + meta-offset(4 bytes)
  // 写入sst文件
  FileObj file = FileObj::create_and_write(path, file_content);

  // 构建SST描述类
  sst->file = std::move(file);
  sst->meta_entries = this->meta_entries;
  sst->sst_id = sst_id;
  sst->first_key = this->first_key;
  sst->last_key = this->last_key;

  return sst;
}
} // namespace tiny_lsm
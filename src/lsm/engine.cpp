#include "../../include/lsm/engine.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/logger/logger.h"
#include "../../include/lsm/level_iterator.h"
#include "../../include/sst/concact_iterator.h"
#include "../../include/sst/sst.h"
#include "../../include/sst/sst_iterator.h"
#include "iterator/iterator.h"
#include "lsm/two_merge_iterator.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tiny_lsm {

// *********************** LSMEngine ***********************
LSMEngine::LSMEngine(std::string path) : data_dir(path) {
  // 初始化日志
  init_spdlog_file();
  // TODO: Lab 4.2 引擎初始化
  // 要把sst文件的内容读出来

  // 初始化BlockCache
  block_cache = std::make_shared<BlockCache>(
      TomlConfig::getInstance().getLsmBlockCacheCapacity(),
      TomlConfig::getInstance().getLsmBlockCacheK());

  for (const auto &file : std::filesystem::directory_iterator(path)) {
    std::string filename = file.path().filename().string();
    size_t dot_pos = filename.find('.');
    // 提取level
    std::string level_str =
        filename.substr(dot_pos + 1, filename.length() - 1 - dot_pos);
    size_t level = std::stoull(level_str);

    //提取 sst id
    std::string id_str = filename.substr(4, dot_pos - 1 - 4 + 1);
    size_t sst_id = std::stoull(id_str);

    //得到 sst文件的完整路径
    std::string sst_path = get_sst_path(sst_id, level);

    // 读取SST文件
    auto sst = SST::open(sst_id, FileObj::open(sst_path, false), block_cache);

    // 完善engine类的相关信息
    this->ssts[sst_id] = sst;
    this->level_sst_ids[level].push_back(sst_id);
  }

  for (auto &[level, sst_id_list] : level_sst_ids) {
    std::sort(sst_id_list.begin(), sst_id_list.end());
    if (level == 0) {
      // 其他 level 的 sst 都是没有重叠的, 且 id 小的表示 key
      // 排序在前面的部分, 不需要 reverse
      std::reverse(sst_id_list.begin(), sst_id_list.end());
    }
  }
}

LSMEngine::~LSMEngine() = default;

/**
 * @brief 在LSM引擎中查找指定键的值（完整版本）
 *
 * 该函数按照LSM树的层级结构依次查找给定的键，查找顺序为：
 * 1. 内存表（MemTable）
 * 2. Level 0的SST文件（按SST ID从大到小顺序，新的优先）
 * 3. 其他层级的SST文件（使用二分查找定位可能包含该键的SST文件）
 *
 * @param key 要查找的键
 * @param tranc_id 事务ID，用于可见性判断
 * @return std::optional<std::pair<std::string, uint64_t>>
 *         如果找到且未被删除，返回键值对和事务ID；如果键不存在或已被删除，返回std::nullopt
 *
 * @note 查找过程中采用多级缓存策略：内存表->L0 SST->其他层级SST
 * @note Level 0的SST文件按SST ID降序查找（新的数据优先）
 * @note 越晚刷入磁盘的sst文件优先查询
 * @note 其他层级的SST文件使用二分查找优化搜索效率
 * @note 空值表示该键已被标记删除（逻辑删除）
 * @note 函数执行期间会持有SST文件的读锁以保证一致性
 * @note 会记录详细的跟踪日志，包括查找结果和来源信息
 *
 * @see MemTable::get()
 * @see SST::get()
 */
std::optional<std::pair<std::string, uint64_t>>
LSMEngine::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.2 查询
  // 1. 先在 memtable 中查询
  auto sk_iter = memtable.get(key, tranc_id);
  if (sk_iter.is_valid()) {
    const auto &value = sk_iter.get_value();
    if (value != "") {
      std::pair<std::string, uint64_t> res{value, sk_iter.get_tranc_id()};
      return res;
    } else {
      spdlog::trace("LSMEngine--"
                    "get({},{}): key is deleted, returning "
                    "from memtable",
                    key, tranc_id);
      return std::nullopt;
    }
  }

  // 2. 在 engine 管理的sst文件中查询
  for (auto sst_id : this->level_sst_ids[0]) {
    auto sst = this->ssts[sst_id];
    auto sst_it = sst->get(key, tranc_id);
    if (sst_it != sst->end()) {
      const auto &value = sst_it->second;
      if (value == "") {
        spdlog::trace("LSMEngine--"
                      "get({},{}): key is deleted or do not "
                      "exist , returning "
                      "from l0 sst{}",
                      key, tranc_id, sst_id);
        return std::nullopt;
      }
      return std::pair<std::string, uint64_t>(value, tranc_id);
    }
  }
  return std::nullopt;
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
LSMEngine::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {
  // TODO: Lab 4.2 批量查询

  return {};
}

std::optional<std::pair<std::string, uint64_t>>
LSMEngine::sst_get_(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.2 sst 内部查询
  return std::nullopt;
}

/**
 * @brief 将键值对插入到LSM引擎中
 *
 * 将指定的键值对插入到内存表(memtable)中，并根据内存表大小决定是否触发刷新到磁盘操作。
 * 如果内存表的大小超过配置的阈值，会自动执行flush操作将内存表数据写入磁盘。
 *
 * @param[in] key 要插入的键
 * @param[in] value 要插入的值
 * @param[in] tranc_id 事务ID，用于标识插入操作所属的事务
 *
 * @return uint64_t 如果触发了flush操作，返回flush操作的结果；否则返回0
 *
 * @note 插入操作首先被写入内存表，然后检查内存表大小是否超过配置限制
 * @note
 * 当内存表大小超过TomlConfig中配置的lsm_tol_mem_size_limit时，会触发flush操作
 * @note 使用spdlog进行trace级别的日志记录，便于调试和追踪
 *
 * @see LSMEngine::flush()
 * @see MemTable::put()
 * @see TomlConfig::getLsmTolMemSizeLimit()
 */
uint64_t LSMEngine::put(const std::string &key, const std::string &value,
                        uint64_t tranc_id) {
  // TODO: Lab 4.1 插入
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  memtable.put(key, value, tranc_id);
  if (memtable.get_total_size() >=
      TomlConfig::getInstance().getLsmTolMemSizeLimit()) {
    auto new_sst_id = this->flush();
    return new_sst_id;
  }

  return 0;
}

uint64_t LSMEngine::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  // TODO: Lab 4.1 批量插入
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  return 0;
}
uint64_t LSMEngine::remove(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.1 删除
  // ? 在 LSM 中，删除实际上是插入一个空值
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  memtable.remove(key, tranc_id);
  if (memtable.get_total_size() >=
      TomlConfig::getInstance().getLsmTolMemSizeLimit()) {
    auto new_sst_id = this->flush();
    return new_sst_id;
  }
  return 0;
}

uint64_t LSMEngine::remove_batch(const std::vector<std::string> &keys,
                                 uint64_t tranc_id) {
  // TODO: Lab 4.1 批量删除
  // ? 在 LSM 中，删除实际上是插入一个空值
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  uint64_t sst_id = 0;
  for (const auto &key : keys) {
    sst_id = remove(key, tranc_id);
  }
  return sst_id;
}

void LSMEngine::clear() {
  memtable.clear();
  level_sst_ids.clear();
  ssts.clear();
  // 清空当前文件夹的所有内容
  try {
    for (const auto &entry : std::filesystem::directory_iterator(data_dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      std::filesystem::remove(entry.path());

      spdlog::info("LSMEngine--"
                   "clear file {} successfully.",
                   entry.path().string());
    }
  } catch (const std::filesystem::filesystem_error &e) {
    // 处理文件系统错误
    spdlog::error("Error clearing directory: {}", e.what());
  }
}

/**
 * @brief 将内存表中最后一张跳表的数据刷新到磁盘，形成新的sst文件
 *
 * 将当前内存表的数据刷新到磁盘，生成新的SSTable文件。在执行刷新前会检查L0层的SSTable数量是否超过阈值，
 * 如果超过则先执行L0到L1的合并压缩操作。刷新过程包括创建新的SSTable文件、更新内存索引和事务状态。
 *
 * @return uint64_t 返回新刷入的SSTable中最大的事务ID
 *
 * @note 如果内存表为空，则直接返回0，不执行任何操作
 * @note 刷新过程需要获取SSTable的写锁，确保线程安全
 * @note
 * 在刷新前会检查L0层SSTable数量，如果超过配置的level_ratio，会先执行full_compact操作
 * @note 刷新完成后会更新事务管理器，标记已刷新的事务ID
 * @note 调用Memtable的flush_last()，将内存表中最后一张跳表的数据刷新到磁盘
 *
 * @see LSMEngine::full_compact()
 * @see MemTable::flush_last()
 * @see SSTBuilder
 * @see TransactionManager::add_flushed_tranc_id()
 */
uint64_t LSMEngine::flush() {
  // TODO: Lab 4.1 刷盘形成sst文件
  if (level_sst_ids.find(0) != level_sst_ids.end() &&
      level_sst_ids[0].size() >=
          TomlConfig::getInstance().getLsmSstLevelRatio()) {
    full_compact(0);
  }

  size_t new_sst_id = next_sst_id++;

  SSTBuilder sst_builder(TomlConfig::getInstance().getLsmBlockSize(), false);
  auto path = get_sst_path(new_sst_id, 0);
  auto sst = this->memtable.flush_last(sst_builder, path, new_sst_id,
                                       this->block_cache);
  this->ssts[new_sst_id] = sst;
  this->level_sst_ids[0].push_front(
      new_sst_id); // 越晚刷入的优先查询，详见LSMEngine::get()
  return new_sst_id;
}

std::string LSMEngine::get_sst_path(size_t sst_id, size_t target_level) {
  // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
     << '.' << target_level;
  return ss.str();
}

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMEngine::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO: Lab 4.7 谓词查询
  return std::nullopt;
}

Level_Iterator LSMEngine::begin(uint64_t tranc_id) {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}

Level_Iterator LSMEngine::end() {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}

/**
 * @brief 执行全量压缩操作，将指定层级的SST文件合并到下一层级
 *
 * 该函数负责LSM树的全量压缩过程，将源层级的所有SST文件与目标层级的SST文件进行合并，
 * 生成新的SST文件并更新层级结构。如果目标层级文件数量超过阈值，会递归触发更深层级的压缩。
 *
 * @param src_level 源层级编号，该层级的所有SST文件将被压缩到下一层级
 *
 * @note
 * 如果src_level为0，由于L0层的SST文件键范围可能存在重叠，会使用特殊的L0到L1压缩逻辑
 * @note 压缩过程中会删除旧的SST文件，并添加新生成的SST文件到目标层级
 * @note 函数会更新当前最大层级记录，确保层级信息的准确性
 * @note 压缩完成后，目标层级的SST文件ID列表会按升序排序
 *
 * @warning 该操作会修改LSM树的层级结构，调用前需要确保没有其他并发操作
 *
 * 处理流程：
 * 1. 检查下一层级是否需要递归压缩
 * 2. 记录源层级和目标层级的当前SST文件ID
 * 3. 根据源层级选择不同的压缩策略（L0特殊处理或普通层级处理）
 * 4. 删除旧的SST文件记录和物理文件
 * 5. 更新层级信息并添加新生成的SST文件
 */
void LSMEngine::full_compact(size_t src_level) {
  // TODO: Lab 4.5 负责完成整个 full compact
  // ? 你可能需要控制`Compact`流程需要递归地进行

  // 判断是否需要继续递归
  if (level_sst_ids[src_level].size() >
      TomlConfig::getInstance().getLsmSstLevelRatio()) {
    full_compact(src_level + 1);
  }

  // 将源层级的所有SST文件与目标层级的SST文件进行合并, 并生成新的SST文件
  spdlog::debug("LSMEngine--"
                "Compaction: Starting full compaction from level{} to level{}",
                src_level, src_level + 1);
  std::vector<std::shared_ptr<SST>> new_ssts;
  if (src_level == 0) {
    std::vector<size_t> l0_ids(this->level_sst_ids[0].begin(),
                               this->level_sst_ids[0].end());
    std::vector<size_t> l1_ids(this->level_sst_ids[1].begin(),
                               this->level_sst_ids[1].end());
    new_ssts = full_l0_l1_compact(l0_ids, l1_ids);
  } else {
    std::vector<size_t> lx_ids(this->level_sst_ids[src_level].begin(),
                               this->level_sst_ids[src_level].end());
    std::vector<size_t> ly_ids(this->level_sst_ids[src_level + 1].begin(),
                               this->level_sst_ids[src_level + 1].end());
    new_ssts = full_common_compact(lx_ids, ly_ids, src_level + 1);
  }

  // 完成 compact 后移除旧的sst记录(包括源层级和目标层级)
  auto sst_ids = this->level_sst_ids[src_level];
  for (auto sst_id : sst_ids) {
    auto sst = this->ssts[sst_id];
    sst->del_sst();
    this->ssts.erase(sst_id);
  }
  sst_ids = this->level_sst_ids[src_level + 1];
  for (auto sst_id : sst_ids) {
    auto sst = this->ssts[sst_id];
    sst->del_sst();
    this->ssts.erase(sst_id);
  }
  //
  std::deque<size_t>().swap(level_sst_ids[src_level]);
  std::deque<size_t>().swap(level_sst_ids[src_level + 1]);

  // 添加新的sst到对应的层级记录(src_level+1)
  for (auto new_sst : new_ssts) {
    auto new_sst_id = new_sst->get_sst_id();
    this->level_sst_ids[src_level + 1].push_back(new_sst_id);
    this->ssts[new_sst_id] = new_sst;
  }
  spdlog::debug("LSMEngine--"
                "Compaction: Finished compaction. New SSTs added at level{}",
                src_level + 1);
}

std::vector<std::shared_ptr<SST>>
LSMEngine::full_l0_l1_compact(std::vector<size_t> &l0_ids,
                              std::vector<size_t> &l1_ids) {
  // TODO: Lab 4.5 负责完成 l0 和 l1 的 full compact
  std::vector<std::shared_ptr<SST>> l0_ids_ssts;
  std::vector<std::shared_ptr<SST>> l1_ids_ssts;
  std::vector<SstIterator> l0_iters;
  //
  for (auto id : l0_ids) {
    l0_ids_ssts.push_back(this->ssts[id]);
    l0_iters.push_back(this->ssts[id]->begin(0));
  }
  for (auto id : l1_ids) {
    l1_ids_ssts.push_back(this->ssts[id]);
  }
  auto [l0_begin, l0_end] = SstIterator::merge_sst_iterator(l0_iters, 0);
  auto l1_iter = std::make_shared<ConcactIterator>(l1_ids_ssts, 0);
  auto l0_iter = std::make_shared<HeapIterator>(l0_begin);
  TwoMergeIterator merger_iter(l0_iter, l1_iter, 0);
  return gen_sst_from_iter(merger_iter, LSMEngine::get_sst_size(0), 1);
}

/**
 * @brief 执行完整的通用压缩操作
 *
 * 该函数执行跨层级的SST文件压缩合并操作，将源层级（lx）和目标层级（ly）的
 * 指定SST文件进行归并排序，生成新的SST文件到目标层级。
 *
 * @param lx_ids 源层级（lx）要参与压缩的SST文件ID列表
 * @param ly_ids 目标层级（ly）要参与压缩的SST文件ID列表
 * @param level_y 目标层级编号
 * @return std::vector<std::shared_ptr<SST>> 压缩后生成的新SST文件列表
 *
 * @note 使用连接迭代器（ConcactIterator）将同层级的多个SST文件连接
 * @note 使用双路归并迭代器（TwoMergeIterator）对两个层级的文件进行归并排序
 * @note 如果目标层级是底层（没有下一级），可以清理删除标记以节省空间
 * @note 需要补全已完成事务的过滤逻辑
 *
 * @see ConcactIterator
 * @see TwoMergeIterator
 * @see LSMEngine::gen_sst_from_iter()
 * @see LSMEngine::get_sst_size()
 */
std::vector<std::shared_ptr<SST>>
LSMEngine::full_common_compact(std::vector<size_t> &lx_ids,
                               std::vector<size_t> &ly_ids, size_t level_y) {
  // TODO: Lab 4.5 负责完成其他相邻 level 的 full compact
  std::vector<std::shared_ptr<SST>> lx_iters;
  std::vector<std::shared_ptr<SST>> ly_iters;

  for (auto id : lx_ids) {
    lx_iters.push_back(ssts[id]);
  }
  for (auto id : ly_ids) {
    ly_iters.push_back(ssts[id]);
  }
  std::shared_ptr<ConcactIterator> old_lx_begin_ptr =
      std::make_shared<ConcactIterator>(lx_iters, 0);

  std::shared_ptr<ConcactIterator> old_ly_begin_ptr =
      std::make_shared<ConcactIterator>(ly_iters, 0);
  TwoMergeIterator lx_ly_begin(old_lx_begin_ptr, old_ly_begin_ptr, 0);
  // TODO:如果目标 level 的下一级 level+1 不存在, 则为底层的level,
  // 可以清理掉删除标记
  return gen_sst_from_iter(lx_ly_begin, LSMEngine::get_sst_size(level_y),
                           level_y);
}

/**
 * @brief 从迭代器生成SST文件
 *
 * 该函数从给定的迭代器中读取键值对数据，按照目标SST文件大小分批构建SST文件。
 * 当累积的数据量达到目标大小时，会生成一个新的SST文件并重置构建器。
 *
 * @param iter 输入迭代器，提供键值对数据源
 * @param target_sst_size 目标SST文件大小阈值
 * @param target_level 生成的SST文件所属的层级
 * @return std::vector<std::shared_ptr<SST>> 生成的新SST文件列表
 *
 * @note 迭代器遍历过程中会持续添加数据到SST构建器
 * @note 当累积数据达到目标大小时会立即生成一个SST文件
 * @note 迭代器遍历完成后，如果构建器中还有剩余数据，会生成最后一个SST文件
 * @note 需要补全已完成事务的删除逻辑
 * @note 每个生成的SST文件都有唯一的SST ID和对应的文件路径
 *
 * @see SSTBuilder::add()
 * @see SSTBuilder::build()
 * @see LSMEngine::get_sst_path()
 */
std::vector<std::shared_ptr<SST>>
LSMEngine::gen_sst_from_iter(BaseIterator &iter, size_t target_sst_size,
                             size_t target_level) {
  // TODO: Lab 4.5 实现从迭代器构造新的 SST
  std::vector<std::shared_ptr<SST>> new_ssts;
  auto sst_builder =
      SSTBuilder(TomlConfig::getInstance().getLsmBlockSize(), false);
  //
  while (!iter.is_end()) {
    sst_builder.add((*iter).first, (*iter).second, iter.get_tranc_id());
    if (sst_builder.estimated_size() > target_sst_size) {
      size_t sst_id = next_sst_id++;
      const auto &path = get_sst_path(sst_id, target_level);
      auto new_sst = sst_builder.build(sst_id, path, this->block_cache);
      //
      this->level_sst_ids[target_level].push_front(sst_id);
      this->ssts[sst_id] = new_sst;
      new_ssts.push_back(new_sst);
    }
    ++iter;
  }
  //
  if (new_ssts.empty()) {
    spdlog::error("LSMEngine--"
                  "Compaction: No new sst file has been created");
  }

  return new_ssts;
}

size_t LSMEngine::get_sst_size(size_t level) {
  if (level == 0) {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit();
  } else {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit() *
           static_cast<size_t>(std::pow(
               TomlConfig::getInstance().getLsmSstLevelRatio(), level));
  }
}

// *********************** LSM ***********************
LSM::LSM(std::string path)
    : engine(std::make_shared<LSMEngine>(path)),
      tran_manager_(std::make_shared<TranManager>(path)) {
  // TODO: Lab 5.5 控制WAL重放与组件的初始化
}

LSM::~LSM() {
  flush_all();
  tran_manager_->write_tranc_id_file();
}

std::optional<std::string> LSM::get(const std::string &key, bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  auto res = engine->get(key, tranc_id);

  if (res.has_value()) {
    return res.value().first;
  }
  return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::string>>>
LSM::get_batch(const std::vector<std::string> &keys) {
  // 1. 获取事务ID
  auto tranc_id = tran_manager_->getNextTransactionId();

  // 2. 调用 engine 的批量查询接口
  auto batch_results = engine->get_batch(keys, tranc_id);

  // 3. 构造最终结果
  std::vector<std::pair<std::string, std::optional<std::string>>> results;
  for (const auto &[key, value] : batch_results) {
    if (value.has_value()) {
      results.emplace_back(key, value->first); // 提取值部分
    } else {
      results.emplace_back(key, std::nullopt); // 键不存在
    }
  }

  return results;
}

/**
 * @brief 插入键值对到LSM树中
 *
 * 将指定的键值对插入到LSM存储引擎中，支持事务控制。
 * 如果事务功能被关闭，则使用默认的事务ID 0。
 *
 * @param[in] key 要插入的键
 * @param[in] value 要插入的值
 * @param[in] tranc_off
 * 是否关闭事务功能。如果为true，则不使用事务管理。默认开启事务
 *
 * @note 当tranc_off为false时，会从事务管理器获取新的事务ID
 * @note 实际的数据插入操作委托给底层的存储引擎执行
 *
 * @see LSM::get()
 * @see LSM::remove()
 */
void LSM::put(const std::string &key, const std::string &value,
              bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  engine->put(key, value, tranc_id);
}

void LSM::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put_batch(kvs, tranc_id);
}
void LSM::remove(const std::string &key) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove(key, tranc_id);
}

void LSM::remove_batch(const std::vector<std::string> &keys) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove_batch(keys, tranc_id);
}

void LSM::clear() { engine->clear(); }

void LSM::flush() { auto max_tranc_id = engine->flush(); }

/**
 * @brief 强制刷新所有内存表数据到磁盘
 *
 * 该函数循环检查内存表的总大小，只要内存表中还有数据就持续执行刷新操作。
 * 每次刷新会将当前内存表中最后一张跳表的数据写入SST文件，并更新事务管理器中的最大已刷新事务ID。
 *
 * @note 该函数会阻塞直到所有内存表数据都被刷新到磁盘
 * @note 适用于关闭数据库或执行检查点等需要持久化所有数据的场景
 * @note 每次刷新后都会更新事务管理器的状态
 * @note
 * 通过LSM引擎写入的数据一部分持久化到了磁盘中，另一部分还在内存里(Memtable)
 * @note 在LSM引擎析构时，需要将还未持久化的内存数据刷到磁盘里
 *
 * @see LSMEngine::flush()
 * @see TransactionManager::update_max_flushed_tranc_id()
 */
void LSM::flush_all() {
  while (engine->memtable.get_total_size() > 0) {
    auto max_tranc_id = engine->flush();
    tran_manager_->update_max_flushed_tranc_id(max_tranc_id);
  }
}

LSM::LSMIterator LSM::begin(uint64_t tranc_id) {
  return engine->begin(tranc_id);
}

LSM::LSMIterator LSM::end() { return engine->end(); }

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSM::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  return engine->lsm_iters_monotony_predicate(tranc_id, predicate);
}

// 开启一个事务
std::shared_ptr<TranContext>
LSM::begin_tran(const IsolationLevel &isolation_level) {
  // TODO: xx

  return {};
}

void LSM::set_log_level(const std::string &level) { reset_log_level(level); }
} // namespace tiny_lsm
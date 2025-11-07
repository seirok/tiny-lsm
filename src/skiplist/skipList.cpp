#include "../../include/skiplist/skiplist.h"
#include <bits/stdint-uintn.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace tiny_lsm {

// ************************ SkipListIterator ************************
BaseIterator &SkipListIterator::operator++() {
  // TODO: Lab1.2 任务：实现SkipListIterator的++操作符
  assert(this->current);
  this->current = this->current->forward_[0];
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的==操作符
  if (this->is_end() && other.is_end())
    return true; // 两者均为末尾节点
  if (other.is_end() && !this->is_end())
    return false;
  if (!this->is_end() && !other.is_end())
    return false;
  if ((*other).first == this->current->key_ &&
      (*other).second == this->current->value_) {
    return true;
  } else {
    return false;
  }
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的!=操作符

  if (this->is_end() && other.is_end())
    return false; // 两者均为末尾节点
  if (other.is_end() && !this->is_end())
    return true;
  if (!this->is_end() && other.is_end())
    return true;
  if ((*other).first == this->current->key_ &&
      (*other).second == this->current->value_) {
    return false;
  } else {
    return true;
  }
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的*操作符
  assert(this->current);
  return {this->current->key_, this->current->value_};
}

IteratorType SkipListIterator::get_type() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的get_type
  // ? 主要是为了熟悉基类的定义和继承关系
  return IteratorType::SkipListIterator;
}

bool SkipListIterator::is_valid() const {
  return current && !current->key_.empty();
}
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key_; }
std::string SkipListIterator::get_value() const { return current->value_; }
uint64_t SkipListIterator::get_tranc_id() const { return current->tranc_id_; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  // ? 通过"抛硬币"的方式随机生成层数：
  // ? - 每次有50%的概率增加一层
  // ? - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // ? - 层数范围限制在[1, max_level]之间，避免浪费内存
  // TODO: Lab1.1 任务：插入时随机为这一次操作确定其最高连接的链表层数
  const int kBranch = 2;
  int level = 1;
  while (std::rand() % kBranch == 0 && level < max_level) {
    level++;
  }
  return level;
}

std::shared_ptr<SkipListNode>
SkipList::findGreaterOrEqual(const std::string &key, uint64_t tranc_id,
                             std::vector<std::shared_ptr<SkipListNode>> &prev) {
  int level = current_level - 1;
  auto cur = head;
  while (true) {
    auto next = cur->forward_[level];
    if (next && key > next->key_) {
      cur = next;
    } else {
      prev[level] = cur;
      if (!level) {
        return next;
      } else {
        level--;
      }
    }
  }
}
// 插入或更新键值对
void SkipList::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {

  // spdlog::trace("SkipList--put({}, {}, {})", key, value, tranc_id);

  // TODO: Lab1.1  任务：实现插入或更新键值对
  // ? Hint: 你需要保证不同`Level`的步长从底层到高层逐渐增加
  // ? 你可能需要使用到`random_level`函数以确定层数, 其注释中为你提供一种思路
  // ? tranc_id 为事务id, 现在你不需要关注它, 直接将其传递到 SkipListNode
  // 的构造函数中即可
  int height = random_level();
  auto new_node = std::make_shared<SkipListNode>(key, value, height, tranc_id);

  std::vector<std::shared_ptr<SkipListNode>> prev(max_level);
  auto result = findGreaterOrEqual(key, tranc_id, prev);

  if (result && result->key_ == key) { // 已有相同节点存在
    this->size_bytes += value.size() - result->value_.size();
    result->value_ = value;
    return;
  } else { // 插入新节点
    for (int i = 0; i < height; i++) {
      if (i >= current_level) {
        head->forward_[i] = new_node;
        new_node->backward_[i] = head;
      } else {
        new_node->forward_[i] = prev[i]->forward_[i]; // 后向
        prev[i]->forward_[i] = new_node;

        new_node->backward_[i] = prev[i]; // 前向
        if (new_node->forward_[i]) {
          new_node->forward_[i]->backward_[i] = new_node;
        }
      }
    }
    if (height > current_level) { // 更新current_level
      current_level = height;
    }
    this->size_bytes += key.size() + value.size() + sizeof(uint64_t);
    return;
  }
}

// 查找键值对
SkipListIterator SkipList::get(const std::string &key, uint64_t tranc_id) {
  // spdlog::trace("SkipList--get({}) called", key);
  // ? 你可以参照上面的注释完成日志输出以便于调试
  // ? 日志为输出到你执行二进制所在目录下的log文件夹

  // TODO: Lab1.1 任务：实现查找键值对,
  // TODO: 并且你后续需要额外实现SkipListIterator中的TODO部分(Lab1.2)
  std::vector<std::shared_ptr<SkipListNode>> prev(max_level);
  auto result = findGreaterOrEqual(key, tranc_id, prev);
  if (result && result->key_ == key) {
    return SkipListIterator{result};
  } else {
    return SkipListIterator{};
  }
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
void SkipList::remove(const std::string &key) {
  // TODO: Lab1.1 任务：实现删除键值对
  std::vector<std::shared_ptr<SkipListNode>> prev(max_level);
  auto result = findGreaterOrEqual(key, 0, prev);

  if (result && result->key_ == key) { // 节点存在
    int del_height = result->forward_.size();
    for (int i = 0; i < del_height; i++) {
      prev[i]->forward_[i] = result->forward_[i];
      if (result->forward_[i]) {
        result->forward_[i]->backward_[i] = prev[i];
      } else {
      }
    }

    if (del_height == current_level) { // 更新current_level
      for (int i = del_height - 1; i >= 0; i--) {
        if (head->forward_[i]) {
          current_level = i + 1;
          break;
        } else
          continue;
      }
    }
    this->size_bytes -= (result->key_.size() + result->value_.size() +
                         sizeof(uint64_t)); // 更新内存
  } else {
    return;
  }
}

/**
 * @brief 将跳表中的所有数据刷新到向量中
 *
 * 该函数遍历跳表中的所有节点，将每个节点的键、值和事务ID打包成元组，
 * 并按照跳表的遍历顺序收集到向量中返回。
 *
 * @return std::vector<std::tuple<std::string, std::string, uint64_t>>
 *         包含所有节点数据的向量，每个元素为(key, value, transaction_id)元组
 *
 * @note 该函数目前注释掉了共享锁和日志输出，需要时可以启用
 * @note 遍历顺序为跳表最底层的链表顺序（即按键排序的顺序）
 * @note 返回的数据可以用于后续的SST文件构建或其他持久化操作
 */
std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  // spdlog::debug("SkipList--flush(): Starting to flush skiplist data");

  std::vector<std::tuple<std::string, std::string, uint64_t>> data;
  auto node = head->forward_[0];
  while (node) {
    data.emplace_back(node->key_, node->value_, node->tranc_id_);
    node = node->forward_[0];
  }

  //  spdlog::debug("SkipList--flush(): Flushed {} entries", data.size());

  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward_[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator(); // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器

SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的起始位置
  //  std::cout << "begin preffix" << std::endl;
  auto &&result = iters_monotony_predicate([&](const std::string &key) {
    if (preffix.size() > key.size()) {
      return 1;
    }
    auto match_str = key.substr(0, preffix.size());
    if (match_str == preffix) {
      return 0;
    } else if (match_str > preffix) {
      return -1;
    } else {
      return 1;
    }
    return 0;
  });

  if (result == std::nullopt) {
    return SkipListIterator{};
  } else {
    return result->first;
  }

  // 二分查询
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  // TODO: Lab1.3 任务：实现前缀查询的终结位置
  // std::cout << "end_preffix" << std::endl;
  auto &&result = iters_monotony_predicate([&](const std::string &key) {
    if (prefix.size() > key.size()) {
      return 1;
    }
    auto match_str = key.substr(0, prefix.size());
    if (match_str == prefix) {
      return 0;
    } else if (match_str > prefix) {
      return -1;
    } else {
      return 1;
    }
    return 0;
  });
  if (result == std::nullopt) {
    return SkipListIterator{};
  } else {
    return result->second;
  }
}

// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的下一个位置的迭代器: [left,
// right) ? 如果不存在, 范围nullptr ? 谓词作用于key,
// 且需要保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词 ?
// 谓词对应于区间，谓词决定了哪些term是在区间内的，哪些是在区间左边/右边的 ?
// predicate返回值: ?   0: 满足谓词 ?   >0: 不满足谓词, 需要向右移动 ?   <0:
// 不满足谓词, 需要向左移动 ! Skiplist 中的谓词查询不会进行事务id的判断,
// 需要上层自己进行判断

std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab1.3 任务：实现谓词查询的起始位置
  //  this->print_skiplist();
  int level = current_level - 1;
  std::shared_ptr<SkipListNode> right = head->forward_[level]; // !
  std::shared_ptr<SkipListNode> left = head->forward_[level];  // !
  bool flag = false;
  for (int i = current_level - 1; i >= 0 && !flag; i--) {
    // std::cout << i << std::endl;
    auto cur = head->forward_[i];
    while (true) {
      if (!cur) {
        break;
      }
      int res = predicate(cur->key_);
      if (res == 0) {
        left = cur;
        right = cur;
        flag = true;
        break;
      } else if (res > 0) {
        cur = cur->forward_[i];
      } else {
        break;
      }
    }
  }
  // std::cout << "fdf" << std::endl;
  if (flag) {
    // std::cout << "flag" << std::endl;
    // std::cout << left->key_ << std::endl;
    // std::cout << right->key_ << std::endl;
    while (true) {
      left = left->backward_[0].lock();
      if (left == head || predicate(left->key_) != 0) {
        left = left->forward_[0]; // go back
      } else {
        continue;
      }

      right = right->forward_[0];
      if (!right || predicate(right->key_) != 0) {
        break;
      } else {
        continue;
      }
    }

    return std::make_pair(SkipListIterator(left), SkipListIterator(right));
  } else {
    return std::nullopt;
  }

  return std::nullopt;
}

// ? 打印跳表, 你可以在出错时调用此函数进行调试
void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward_[level];
    while (current) {
      std::cout << current->key_;
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}
} // namespace tiny_lsm
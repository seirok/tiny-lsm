#include "../../include/lsm/two_merge_iterator.h"
#include "logger/logger.h"
#include "spdlog/spdlog.h"

namespace tiny_lsm {

TwoMergeIterator::TwoMergeIterator() {}

/**
 * @brief 双路归并迭代器构造函数
 *
 * 该构造函数初始化双路归并迭代器，对两个输入迭代器进行预处理：
 * 1. 跳过不可见的事务（基于最大事务ID）
 * 2. 跳过迭代器B中与迭代器A重复的键
 * 3. 确定当前应该使用哪个迭代器的数据
 *
 * @param it_a 第一个输入迭代器的共享指针
 * @param it_b 第二个输入迭代器的共享指针
 * @param max_tranc_id 最大事务ID，用于过滤不可见的事务
 *
 * @note 使用移动语义接管迭代器所有权，避免不必要的拷贝
 * @note 初始化过程中会进行事务可见性过滤和重复键处理
 * @note 最终通过choose_it_a()确定当前活跃的迭代器
 *
 * @see TwoMergeIterator::skip_by_tranc_id()
 * @see TwoMergeIterator::skip_it_b()
 * @see TwoMergeIterator::choose_it_a()
 */
TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> it_a,
                                   std::shared_ptr<BaseIterator> it_b,
                                   uint64_t max_tranc_id)
    : it_a(std::move(it_a)), it_b(std::move(it_b)),
      max_tranc_id_(max_tranc_id) {
  // 先跳过不可见的事务
  skip_by_tranc_id();
  skip_it_b();              // 跳过与 it_a 重复的 key
  choose_a = choose_it_a(); // 决定使用哪个迭代器
}

/**
 * @brief 选择应该使用哪个迭代器的数据
 *
 * 该函数用于在双路归并迭代器中决定当前应该使用哪个迭代器的数据。
 * 选择策略基于键的比较：优先选择键较小的迭代器，如果某个迭代器已结束则选择另一个。
 *
 * @return true 应该使用迭代器A的数据
 * @return false 应该使用迭代器B的数据
 *
 * @note 如果迭代器A已结束，则选择迭代器B
 * @note 如果迭代器B已结束，则选择迭代器A
 * @note 如果两个迭代器都未结束，选择键较小的那个
 * @note 比较的是迭代器指向的键值对的键（first成员）
 *
 * @see TwoMergeIterator::next()
 */
bool TwoMergeIterator::choose_it_a() {
  // TODO: Lab 4.4: 实现选择迭代器的逻辑
  if (it_a->is_end()) {
    return false;
  }
  if (it_b->is_end()) {
    return true;
  }
  return (**it_a).first < (**it_b).first; // 比较 key
  return false;
}

void TwoMergeIterator::skip_it_b() {
  if (!it_a->is_end() && !it_b->is_end() && (**it_a).first == (**it_b).first) {
    ++(*it_b);
  }
}

void TwoMergeIterator::skip_by_tranc_id() {
  // TODO: Lab xx
}

BaseIterator &TwoMergeIterator::operator++() {
  // TODO: Lab 4.4: 实现 ++ 重载
  bool ca = this->choose_it_a();
  if (ca) {
    if (!(it_a->is_end())) {
      ++(*it_a);
    }
  } else {
    if (!(it_b->is_end())) {
      ++(*it_b);
    }
  }
  update_current();
  return *this;
}

bool TwoMergeIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 == 重载
  auto oth_ptr = dynamic_cast<const TwoMergeIterator *>(&other);
  if (!oth_ptr) { // 类型不匹配
    return false;
  }
  // 这个要求在迭代器相等运算符函数里就要判断为空时是否相等
  if (this->it_a == oth_ptr->it_a && this->it_b == oth_ptr->it_b) {
    return true;
  }
  return false;
}

bool TwoMergeIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 != 重载
  return !(this->operator==(other));
}

BaseIterator::value_type TwoMergeIterator::operator*() const {
  // TODO: Lab 4.4: 实现 * 重载
  if (!this->current) {
    spdlog::error("Invalid access");
    exit(-1);
  }
  return *this->current;
}

IteratorType TwoMergeIterator::get_type() const {
  return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_tranc_id() const { return max_tranc_id_; }

bool TwoMergeIterator::is_end() const {
  if (it_a == nullptr && it_b == nullptr) {
    return true;
  }
  if (it_a == nullptr) {
    return it_b->is_end();
  }
  if (it_b == nullptr) {
    return it_a->is_end();
  }
  return it_a->is_end() && it_b->is_end();
}

bool TwoMergeIterator::is_valid() const {
  if (it_a == nullptr && it_b == nullptr) {
    return false;
  }
  if (it_a == nullptr) {
    return it_b->is_valid();
  }
  if (it_b == nullptr) {
    return it_a->is_valid();
  }
  return it_a->is_valid() || it_b->is_valid();
}

TwoMergeIterator::pointer TwoMergeIterator::operator->() const {
  // TODO: Lab 4.4: 实现 -> 重载
  update_current();
  return current.get();
}

void TwoMergeIterator::update_current() const {
  // TODO: Lab 4.4: 实现更新缓存键值对的辅助函数
  if (this->it_a->is_end() && this->it_b->is_end()) {
    return;
  }
  if (!choose_a) {
    this->current = std::make_shared<value_type>(this->it_b->operator*());
  } else {
    this->current = std::make_shared<value_type>(this->it_a->operator*());
  }
  return;
}
} // namespace tiny_lsm
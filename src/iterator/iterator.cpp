#include "../../include/iterator/iterator.h"
#include "iostream"
#include <tuple>
#include <vector>
namespace tiny_lsm {

// *************************** SearchItem ***************************
bool operator<(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  if (a.key_ < b.key_)
    return true;
  if (a.key_ == b.key_ && a.idx_ < b.idx_)
    return true;
  else
    return false;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  if (a.key_ > b.key_)
    return true;
  if (a.key_ == b.key_ && a.idx_ > b.idx_)
    return true;
  else
    return false;
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  return true;
}

// *************************** HeapIterator ***************************
HeapIterator::HeapIterator(std::vector<SearchItem> item_vec,
                           uint64_t max_tranc_id)
    : max_tranc_id_(max_tranc_id) {
  // TODO: Lab2.2 实现 HeapIterator 构造函数
  for (const auto &item : item_vec) {
    items.push(item);
  }
  if (items.empty()) {
    this->current = nullptr;
  } else { // 去除前面所有的待删除键值对
    if (items.top().value_ != "") {
      ;
    } else {
      std::string del_key = items.top().key_;
      while (items.top().key_ == del_key) {
        items.pop();
        if (items.empty()) {
          this->current = nullptr;
          return;
        }
        if (items.top().value_ == "") {
          del_key = items.top().key_;
        }
      }
    }
    this->current =
        std::make_shared<value_type>(items.top().key_, items.top().value_);
  }
}

HeapIterator::pointer HeapIterator::operator->() const {
  // TODO: Lab2.2 实现 -> 重载
  return this->current.get();
}

HeapIterator::value_type HeapIterator::operator*() const {
  // TODO: Lab2.2 实现 * 重载
  return *this->current;
}

BaseIterator &HeapIterator::operator++() {
  // TODO: Lab2.2 实现 ++ 重载
  // it当前指向的item一定不是空的
  if (this->items.empty()) {
    this->current = nullptr;
    return *this;
  }
  std::string key = this->items.top().key_;
  //  std::string value = this->items.top().value_;
  do {
    this->items.pop(); // 删除重复的堆顶元素
    if (this->items.empty()) {
      this->current = nullptr;
      return *this;
    }
  } while (this->items.top().key_ == key);
  std::string del_key = "";
  while (true) { // 删掉所有会删除的键值对
    const auto &top_item = this->items.top();
    if (top_item.key_ == del_key) {
      ;
    } else if (top_item.value_ == "") {
      del_key = top_item.key_;
    } else {
      break;
    }
    this->items.pop();
    if (this->items.empty()) {
      this->current = nullptr;
      return *this;
    }
  }

  const auto &top_item = this->items.top(); // 下一个真正会迭代到的元素
  this->current->first = top_item.key_;
  this->current->second = top_item.value_;
  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 == 重载
  if (const HeapIterator *p = dynamic_cast<const HeapIterator *>(&other)) {
    if (!this->current && !p->current) {
      return true;
    }
    if (!this->current || !p->current) {
      return false;
    }

    if (this->current->first == p->current->first) {
      return true;
    } else {
      return false;
    }
  } else {
    std::cout << "HeapIterator::operator==: Unmatched type" << std::endl;
    return false;
  }
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 != 重载
  if (const HeapIterator *p = dynamic_cast<const HeapIterator *>(&other)) {
    if (!p->current && !this->current) {
      return false;
    }
    if ((p->current && !this->current) || (!p->current && this->current)) {
      return true;
    }
    if (this->current->first == p->current->first) {
      return false;
    } else {
      return true;
    }
  } else {
    return true;
  }
}

bool HeapIterator::top_value_legal() const {
  // TODO: Lab2.2 判断顶部元素是否合法
  // ? 被删除的值是不合法
  // ? 不允许访问的事务创建或更改的键值对不合法(暂时忽略)

  return true;
}

void HeapIterator::skip_by_tranc_id() {
  // TODO: Lab2.2 后续的Lab实现, 只是作为标记提醒
}

bool HeapIterator::is_end() const { return items.empty(); }
bool HeapIterator::is_valid() const { return !items.empty(); }

void HeapIterator::update_current() const {
  // current 缓存了当前键值对的值, 你实现 -> 重载时可能需要
  // TODO: Lab2.2 更新当前缓存值
}

IteratorType HeapIterator::get_type() const {
  return IteratorType::HeapIterator;
}

uint64_t HeapIterator::get_tranc_id() const { return max_tranc_id_; }
} // namespace tiny_lsm
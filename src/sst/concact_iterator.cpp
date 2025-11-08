#include "../../include/sst/concact_iterator.h"
#include "spdlog/spdlog.h"
#include "sst/sst_iterator.h"

namespace tiny_lsm {

ConcactIterator::ConcactIterator(std::vector<std::shared_ptr<SST>> ssts,
                                 uint64_t tranc_id)
    : ssts(ssts), cur_iter(nullptr, tranc_id), cur_idx(0),
      max_tranc_id_(tranc_id) {
  if (!this->ssts.empty()) {
    cur_iter = ssts[0]->begin(max_tranc_id_);
  }
}

BaseIterator &ConcactIterator::operator++() {
  // TODO: Lab 4.3 自增运算符重载
  ++this->cur_iter;
  if (this->cur_iter.is_end()) {
    this->cur_idx++;
    if (this->cur_idx < this->ssts.size()) {
      this->cur_iter = SstIterator(this->ssts[cur_idx], max_tranc_id_);
    } else { // 已经遍历完了该层level所有的sst
      ;
    }
  }
  return *this;
}

bool ConcactIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  auto other_ptr = dynamic_cast<const ConcactIterator *>(&other);
  if (other_ptr == nullptr) { // 类型不匹配
    return false;
  }
  if (other_ptr->cur_idx != this->cur_idx) { // 需要指向同一个sst文件
    return false;
  }

  if (other_ptr->cur_iter != this->cur_iter) {
    return false;
  }
  return true;
}

bool ConcactIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  return !(this->operator==(other));
}

ConcactIterator::value_type ConcactIterator::operator*() const {
  // TODO: Lab 4.3 解引用运算符重载
  if (this->is_end()) { // 越界访问
    spdlog::error("Out-of-bounds access");
    exit(-1);
  }
  return *(this->cur_iter);
}

IteratorType ConcactIterator::get_type() const {
  return IteratorType::ConcactIterator;
}

uint64_t ConcactIterator::get_tranc_id() const { return max_tranc_id_; }

bool ConcactIterator::is_end() const {
  return cur_iter.is_end() || !cur_iter.is_valid();
}

bool ConcactIterator::is_valid() const {
  return !cur_iter.is_end() && cur_iter.is_valid();
}

ConcactIterator::pointer ConcactIterator::operator->() const {
  // TODO: Lab 4.3 ->运算符重载
  if (this->is_end()) { // 越界访问
    spdlog::error("Out-of-bounds access");
    exit(-1);
  }
  return this->operator->();
}

std::string ConcactIterator::key() { return cur_iter.key(); }

std::string ConcactIterator::value() { return cur_iter.value(); }
} // namespace tiny_lsm
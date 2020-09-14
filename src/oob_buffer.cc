//
// Created by Ankush J on 9/11/20.
//

#include "oob_buffer.h"

namespace pdlfs {
OobBuffer::OobBuffer() { buf_.reserve(kMaxOobSize); }

bool OobBuffer::OutOfBounds(float prop) {
  if (not range_set_) return true;

  return (prop < range_min_ or prop > range_max_);
}

int OobBuffer::Insert(particle_mem_t& item) {
  int rv = 0;

  float prop = item.indexed_prop;

  if (range_set_ and prop > range_min_ and prop < range_max_) {
    rv = -1;
    return rv;
  }

  if (buf_.size() >= kMaxOobSize) {
    return -1;
  }

  buf_.push_back(item);

  return rv;
}

size_t OobBuffer::Size() const { return buf_.size(); }

int OobBuffer::SetRange(float range_min, float range_max) {
  range_min_ = range_min;
  range_max_ = range_max;
  range_set_ = true;

  return 0;
}

int OobBuffer::GetPartitionedProps(std::vector<float>& left,
                                   std::vector<float>& right) {
  for (auto it = buf_.cbegin(); it != buf_.cend(); it++) {
    float prop = it->indexed_prop;
    if ((not range_set_) or (range_set_ and prop < range_min_)) {
      left.push_back(prop);
    } else {
      right.push_back(prop);
    }
  }

  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());

  return 0;
}

OobFlushIterator::OobFlushIterator(OobBuffer& buf) : buf_(buf) {
  buf_len_ = buf_.buf_.size();
}

int OobFlushIterator::PreserveCurrent() {
  int rv = 0;

  if (preserve_idx_ > flush_idx_) {
    /* can't preserve ahead of flush */
    return -1;
  }

  buf_.buf_[preserve_idx_++] = buf_.buf_[flush_idx_];
  return rv;
}

particle_mem_t& OobFlushIterator::operator*() {
  if (flush_idx_ < buf_len_) {
    return buf_.buf_[flush_idx_];
  } else {
    /* iterator is out of bounds, do the safest possible thing */
    return buf_.buf_[0];
  }
}

particle_mem_t& OobFlushIterator::operator++() {
  particle_mem_t& rv = buf_.buf_[flush_idx_];

  if (flush_idx_ < buf_len_) {
    flush_idx_++;
  }

  return rv;
}

bool OobFlushIterator::operator==(size_t& other) {
  return (flush_idx_ == other);
}

bool OobFlushIterator::operator!=(size_t& other) {
  return (flush_idx_ != other);
}

OobFlushIterator::~OobFlushIterator() { buf_.buf_.resize(preserve_idx_); }
}  // namespace pdlfs
#pragma once

namespace util {

/**
 *
 * @tparam T is the type of the stored pointer.
 * @tparam Lockable must have lock() and unlock()
 */
template <typename T, typename Lockable>
class ScopeExclusivePtr {
 public:
  ScopeExclusivePtr(T* data, Lockable* lock = nullptr) noexcept
      : data_(data), lock_(lock) {}

  ~ScopeExclusivePtr() noexcept {
    if (lock_) {
      lock_->unlock();
    }
  }

  T* get() { return data_; }
  T& operator*() { return *data_; }
  T* operator->() { return data_; }

  operator bool() { return data_ != nullptr; }

 private:
  T* data_;
  Lockable* lock_;
};

}  // namespace slurmx
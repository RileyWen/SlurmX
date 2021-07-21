#pragma once

#include <boost/thread/lock_guard.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace slurmx {

using mutex = boost::mutex;
using lock_guard = boost::lock_guard<boost::mutex>;

using recursive_mutex = boost::recursive_mutex;
using recursive_lock_guard= boost::lock_guard<boost::recursive_mutex>;

using rw_mutex = boost::shared_mutex;
using read_lock_guard = boost::shared_lock_guard<boost::shared_mutex>;
using write_lock_guard = boost::lock_guard<boost::shared_mutex>;

}  // namespace slurmx
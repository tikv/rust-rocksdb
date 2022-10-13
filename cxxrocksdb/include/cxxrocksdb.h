#pragma once

#include "rocksdb/db.h"
#include "crocksdb/c.h"
#include "rust/cxx.h"
#include "rocksdb/async_future.h"

#include <memory>
#include <tuple>
#include <unordered_map>
#include <set>

#include <sys/uio.h>

struct CRocksDB;
struct RustStatus;
struct Async_result;

using ROCKSDB_NAMESPACE::Async_future;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ReadTier = ROCKSDB_NAMESPACE::ReadTier;
using Submit_queue = Async_future::Submit_queue;
using Return_type = Async_future::Promise_type::Return_type;

struct Async_reader {
  Async_reader() = delete;
  Async_reader(Async_reader&&) = delete;
  Async_reader(const Async_reader&) = delete;
  Async_reader& operator=(Async_reader&&) = delete;
  Async_reader& operator=(const Async_reader&) = delete;

  Async_reader(rocksdb::DB *db, size_t io_uring_size);

  ~Async_reader() noexcept;

  /** Reap entries from the io_uring completion queue (CQ).
  @return Number of processed CQEs */
  uint32_t io_uring_reap_cq() const;

  /** Peek and check if there are any CQEs to process.
  @return true if there are CQEs in the CQ. */
  bool io_uring_peek_cq() const;

  Async_result get(const ReadOptions *ropts, rust::String k) const;

  void setup_io_uring_sq_handler(ReadOptions *ropts) const;

  uint32_t pending_io_uring_sqe_count() const {
    return m_n_pending_sqe.load();
  }

  static RustStatus get_result(Async_result async_result, rust::String &v);

  private:
  using Promise = Async_future::promise_type;

  static void schedule_task(Promise* promise) noexcept;

private:
  struct IO_key {
    bool operator==(const IO_key& rhs) const {
      return m_fd == rhs.m_fd && m_off == rhs.m_off;
    }

    int m_fd{-1};
    off_t m_off{};
  };

  struct IO_key_hash {
    size_t operator()(const IO_key &io_key) const noexcept {
      return io_key.m_fd ^ io_key.m_off;
    }
  };

  using IO_value = std::unordered_set<size_t>;

  /** All data members are mutable so that we can use const functions.
  This allows us to use std::shared_ptr from Rust with an immutable
  reference. */
  mutable rocksdb::DB *m_db{};
  mutable std::atomic<int> m_n_pending_sqe{};
  mutable std::shared_ptr<io_uring> m_io_uring{};
  mutable std::shared_ptr<Submit_queue> m_submit_queue{};
  mutable std::unordered_map<IO_key, IO_value, IO_key_hash> m_pending_io{};
};

std::shared_ptr<Async_reader> new_async_reader(CRocksDB* db, uint32_t io_uring_size);

RustStatus get_async_result(Async_result async_result, rust::String &v);

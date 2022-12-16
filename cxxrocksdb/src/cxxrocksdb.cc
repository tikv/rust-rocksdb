#include "cxxrocksdb.h"
#include "cxxrocksdb/src/lib.rs.h"

#include <algorithm>

using ROCKSDB_NAMESPACE::Async_future;
using ROCKSDB_NAMESPACE::ReadOptions;

using Submit_queue = rocksdb::Async_future::Submit_queue;

Async_reader::~Async_reader() noexcept {
  io_uring_queue_exit(m_io_uring.get());
}

Async_reader::Async_reader(rocksdb::DB* db, size_t io_uring_size) 
        : m_db(db),
          m_io_uring(new io_uring) {
  auto ret = io_uring_queue_init(io_uring_size, m_io_uring.get(), 0);

  if (ret < 0) {
    throw "io_uring_queue_init failed";
  }

  m_submit_queue = std::make_shared<Submit_queue>(
    [this](Async_future::IO_ctx *ctx, int fd, off_t off, Submit_queue::Ops op) -> Async_future {
      using Status = ROCKSDB_NAMESPACE::Status;
      using SubCode = Status::SubCode;
      using IOStatus = ROCKSDB_NAMESPACE::IOStatus;

      ctx->m_fd = fd;
      ctx->m_off = off;

      assert(op == Submit_queue::Ops::Read);

      IO_key io_key{fd, off};
      auto &iovs{ctx->m_iov};
      std::vector<iovec> new_iov{};
      auto it{m_pending_io.find(io_key)};

      if (it == m_pending_io.end()) {
        IO_value len_to_read{};

      	for (const auto &io : iovs) {
          len_to_read.insert(io.iov_len);
      	}

        m_pending_io.insert(it, {io_key, len_to_read});
	new_iov = std::move(iovs);
      } else {
        IO_value new_io_len{};
        auto &pending_io_len{it->second};

	new_iov.resize(iovs.size());

        for (const auto &iov : iovs) {
          for (auto len : pending_io_len) {
            if (iov.iov_len != len) {
	      new_iov.push_back(iov);
              new_io_len.insert(iov.iov_len);
            }
          }
        }

	pending_io_len.insert(new_io_len.begin(), new_io_len.end());
      }

      auto io_uring{m_io_uring.get()};
      auto sqe{io_uring_get_sqe(io_uring)};

      if (sqe == nullptr) {
        co_return IOStatus::IOError(SubCode::kIOUringSqeFull);
      } else {
        if (!new_iov.empty()) {
          io_uring_prep_readv(sqe, fd, new_iov.data(), new_iov.size(), off);
          io_uring_sqe_set_data(sqe, ctx);
	} else {
          // FIXME: Get rid of the nop. We should not be doing any
	  // io_uring operation at all for duplicate requests.
          io_uring_prep_nop(sqe);
          io_uring_sqe_set_data(sqe, reinterpret_cast<Async_future::IO_ctx*>(((uintptr_t) ctx) | 1));
        }

        const auto ret = io_uring_submit(io_uring);

        if (ret < 0) {
          // FIXME: Error handling.
          auto msg{strerror(-ret)};
	  std::cout << "error: " << msg << std::endl;
          co_return IOStatus::IOError(SubCode::kIOUringSubmitError, msg);
        } else {
   	  m_n_pending_sqe.fetch_add(1, std::memory_order_seq_cst);
          co_await Async_future(true, ctx);
          co_return IOStatus::OK();
        }
      }
    });
}

void Async_reader::setup_io_uring_sq_handler(ReadOptions *ropts) const {
  ropts->submit_queue = m_submit_queue;

  // FIXME: Hack it for now.
  ropts->verify_checksums = true;
  ropts->read_tier = ReadTier::kPersistedTier;
}

void Async_reader::schedule_task(Promise* promise) noexcept {
  auto h{std::coroutine_handle<Promise>::from_promise(*promise)};
  h.resume();
}

bool Async_reader::io_uring_peek_cq() const {
  io_uring_cqe* cqe{};
  auto io_uring{m_io_uring.get()};

  return io_uring_peek_cqe(io_uring, &cqe) == 0;
}

uint32_t Async_reader::io_uring_reap_cq() const {
  io_uring_cqe* cqe{};
  uint32_t n_processed{};
  auto io_uring{m_io_uring.get()};

  while (io_uring_peek_cqe(io_uring, &cqe) == 0) {
    // FIXME: Error handling, short reads etc.
    if (cqe->res >= 0) {
      Async_future::IO_ctx *ctx{};
      auto c = (uintptr_t) io_uring_cqe_get_data(cqe);
      
      // To catch reuse of the CQE.
      cqe->user_data = 0xdeadbeef;

      if (c & 1) {
        ctx = reinterpret_cast<Async_future::IO_ctx*>(c & ~1);

        const auto &iov{ctx->m_iov};
        IO_key io_key{ctx->m_fd, ctx->m_off};
        auto it{m_pending_io.find(io_key)};

        if (it != m_pending_io.end()) {
          auto &pending_lens{it->second};

          for (const auto &io : iov) {
	    // FIXME: This is very inefficient, we need to do this in-situ
	    std::vector<size_t> to_erase{};

            for (const auto &pending_len : pending_lens) {
	      // FIXME: Check is too simple
              if (pending_len == io.iov_len) {
                to_erase.push_back(pending_len);
              }
            }
	    for (auto len : to_erase) {
	    	pending_lens.erase(len);
            }
          } 
          if (pending_lens.empty()) {
            m_pending_io.erase(it);
          }
        }
      } else {
      	ctx = reinterpret_cast<Async_future::IO_ctx*>(c);
      }

      io_uring_cqe_seen(io_uring, cqe);

      auto promise = ctx->m_promise;

      delete ctx;

      if (promise != nullptr) {
        schedule_task(promise);
      }

      ++n_processed;
    } else {
      assert(false);
    } 

    auto r = m_n_pending_sqe.fetch_sub(1, std::memory_order_seq_cst);
    assert(r >= 1);
  }

  return n_processed;
}

Async_result Async_reader::get(const ReadOptions *ropts, rust::String k) const {

  std::string key{k};

  Async_result async_result{};

  async_result.m_pinnable = new(std::nothrow) PinnableSlice();
  assert(async_result.m_pinnable != nullptr);

  async_result.m_async_reader = this;
  async_result.m_async_future = new Async_future();

  *async_result.m_async_future = m_db->AsyncGet(
    *ropts, m_db->DefaultColumnFamily(), key, async_result.m_pinnable, nullptr);

  return async_result;

}

RustStatus Async_reader::get_result(Async_result async_result, rust::String &v) {
  v = async_result.m_pinnable->ToString();

  const auto status = async_result.m_async_future->status();

  delete async_result.m_pinnable;
  delete async_result.m_async_future;

  return RustStatus{
    (StatusCode) status.code(),
    (StatusSubCode) status.subcode(),
    (StatusSeverity) status.severity()
  };
}

std::shared_ptr<Async_reader> new_async_reader(CRocksDB* rust_db, uint32_t io_uring_size) {
  auto crocksdb = reinterpret_cast<crocksdb_t*>(rust_db);
  auto db = reinterpret_cast<rocksdb::DB*>(crocksdb_get_instance(crocksdb));
  return std::make_shared<Async_reader>(db, io_uring_size);
}

RustStatus get_async_result(Async_result async_result, rust::String &v) {
  return Async_reader::get_result(async_result, v);
}


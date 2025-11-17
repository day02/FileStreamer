// file_stream_server.cpp
//
// Stream ONLY MBO (rtype 0xA0) records from a Databento DBN file over TCP,
// using mmap + zero-copy buffers + Boost.Asio C++20 coroutines build with
// io_uring
//
// Run:
//
//   ./file_stream_server data.dbn 9000
//

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <boost/asio.hpp>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;
using asio::ip::tcp;

// ------------------------------------------------------------
// Streaming configuration
// ------------------------------------------------------------

constexpr std::size_t kDefaultMaxBatchBytes = 4u << 20;  // 4 MB per batch
constexpr std::size_t kDefaultMaxBatchRecords = 4096u;  // max records per batch

struct ServerConfig {
  std::string dbn_path;
  uint16_t port = 0;
  std::size_t max_batch_bytes = kDefaultMaxBatchBytes;
  std::size_t max_batch_records = kDefaultMaxBatchRecords;
};

std::optional<uint16_t> ParsePort(const std::string_view s) {
  int port = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), port);

  if (ec != std::errc() || ptr != s.data() + s.size() || port < 1 ||
      port > 65535)
    return std::nullopt;

  return static_cast<uint16_t>(port);
}

// Argument parsing + server bootstrap
std::optional<ServerConfig> ParseArgs(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <dbn_file> <port>\n";
    return std::nullopt;
  }

  ServerConfig cfg;
  cfg.dbn_path = argv[1];

  const auto port = ParsePort(argv[2]);

  if (!port) {
    std::cerr << "[fatal] invalid port: " << argv[2] << "\n";
    return std::nullopt;
  }

  cfg.port = *port;
  return cfg;
}

// ------------------------------------------------------------
// mmap wrapper
// ------------------------------------------------------------

class MmapRegion {
 public:
  // ---- Constructor: attempts to mmap file read-only ----
  explicit MmapRegion(const std::string& path)
      : fd_(-1), addr_(nullptr), size_(0) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
      std::cerr << "[fatal] open failed for '" << path
                << "': " << std::strerror(errno) << "\n";
      cleanup();
      return;
    }

    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
      std::cerr << "[fatal] fstat failed for '" << path
                << "': " << std::strerror(errno) << "\n";
      cleanup();
      return;
    }

    if (st.st_size == 0) {
      std::cerr << "[fatal] file is empty: '" << path << "'\n";
      cleanup();
      return;
    }

    size_ = static_cast<std::size_t>(st.st_size);

    addr_ = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (addr_ == MAP_FAILED) {
      std::cerr << "[fatal] mmap failed for '" << path
                << "': " << std::strerror(errno) << "\n";
      cleanup();
      return;
    }
  }

  // ---- Destructor: unmap + close if mapped ----
  ~MmapRegion() {
    cleanup();
  }

  // ---- Disable copy and move ----
  MmapRegion(const MmapRegion&) = delete;
  MmapRegion(MmapRegion&&) = delete;
  MmapRegion& operator=(const MmapRegion&) = delete;
  MmapRegion& operator=(MmapRegion&&) = delete;

  // ---- Getters ----
  bool valid() const noexcept {
    return addr_ != nullptr;
  }

  const void* addr() const noexcept {
    return addr_;
  }

  std::size_t size() const noexcept {
    return size_;
  }

 private:
  void cleanup() noexcept {
    if (addr_ && size_) {
      ::munmap(addr_, size_);
    }
    if (fd_ >= 0) {
      ::close(fd_);
    }
    reset();
  }

  void reset() noexcept {
    fd_ = -1;
    addr_ = nullptr;
    size_ = 0;
  }

  int fd_;
  void* addr_;
  std::size_t size_;
};

// ------------------------------------------------------------
// Minimal DBN structures and helpers
// ------------------------------------------------------------
//
// DBN file layout (simplified):
//
//   [ 0..2 ]  magic: "DBN"
//   [ 3    ]  version (uint8)
//   [ 4..7 ]  metadata_length (uint32, little-endian)
//   [ 8...metadata_length) ] metadata bytes
//   [ ... ] records region (sequence of DBN records)
//
// Each DBN record (simplified):
//
//   struct DbnRecordHeader {
//       uint8_t  length_words; // number of 4-byte words (including header)
//       uint8_t  rtype;        // record type (0xA0 = MBO for this example)
//       uint16_t reserved;     // unused here
//       // followed by payload bytes.
//   };
//
// We only care about rtype == 0xA0 (MBO), and we stream each record as a
// contiguous block of bytes over TCP.

static constexpr std::uint8_t RTYPE_MBO = 0xA0;

#pragma pack(push, 1)
struct DbnFileHeaderView {
  char magic[3];                  // 'D', 'B', 'N'
  std::uint8_t version;           // DBN version
  std::uint32_t metadata_length;  // little-endian
  // ... followed by meta data
};

struct DbnRecordHeader {
  std::uint8_t length_words;
  std::uint8_t rtype;
  uint16_t publisher_id;
  uint32_t instrument_id;
};
#pragma pack(pop)

using ByteSpan = std::span<const std::byte>;
// Span of complete record region (into the mmap).
using DbnRecordRegion = ByteSpan;
// Collection of MBO record slices (each slice is a span into the mmap).
using DbnSlices = std::vector<ByteSpan>;

// Returns a view of DbnRecordRegion.
std::optional<DbnRecordRegion> GetDbnFileView(const MmapRegion& region) {
  const auto* base = static_cast<const std::byte*>(region.addr());
  ByteSpan file_span(base, region.size());

  // Need at least a full header.
  if (file_span.size() < sizeof(DbnFileHeaderView)) {
    std::cerr << "[fatal] DBN file too small for header\n";
    return std::nullopt;
  }

  // Reinterpret the first bytes as a DBN file header view.
  const auto* hdr =
      reinterpret_cast<const DbnFileHeaderView*>(file_span.data());

  // Check magic + basic version sanity.
  if (hdr->magic[0] != 'D' || hdr->magic[1] != 'B' || hdr->magic[2] != 'N' ||
      hdr->version != 3) {
    std::cerr << "[fatal] invalid DBN magic/version, expected \"DBN\"\n";
    return std::nullopt;
  }

  const auto record_header_offset =
      sizeof(DbnFileHeaderView) + hdr->metadata_length;

  // Avoid overflow: ensure header_bytes + metadata_length fits in file_span.
  if (record_header_offset >= file_span.size()) {
    std::cerr << "[fatal] DBN metadata length exceeds file size\n";
    return std::nullopt;
  }

  // Build span of records.
  return file_span.subspan(record_header_offset);
}

// Build in-memory view of ONLY MBO records as zero-copy spans.
std::optional<DbnSlices> GetDbnSlices(const DbnRecordRegion& records) {
  const auto* base = records.data();
  const std::size_t size = records.size();

  DbnSlices slices;
  std::size_t offset = 0;

  while (offset + sizeof(DbnRecordHeader) <= size) {
    // Reinterpret the first bytes as a record header view.
    const auto* hdr = reinterpret_cast<const DbnRecordHeader*>(base + offset);

    constexpr auto kWordBytes = 4u;
    const auto words = static_cast<std::size_t>(hdr->length_words);
    const auto rec_bytes = words * kWordBytes;

    // Check that the full record fits in remaining bytes, without overflow.
    if (offset + rec_bytes > size) {
      std::cerr << "[fatal] truncated record at offset " << offset
                << " (length_words=" << words << ", size=" << size
                << ") – aborting parse\n";
      return std::nullopt;
    }

    // Only collect MBO records; skip others.
    if (hdr->rtype == RTYPE_MBO) {
      slices.emplace_back(base + offset, rec_bytes);
    }

    offset += rec_bytes;
  }

  if (offset != size) {
    std::cerr << "[fatal] " << (size - offset)
              << " trailing bytes at end of records region ignored\n";
    return std::nullopt;
  }

  std::cout << "[parser] total record bytes: " << size
            << ", parsed bytes: " << offset << ", MBO slices: " << slices.size()
            << "\n";

  return slices;
}

// ------------------------------------------------------------
// Streaming logic (Boost.Asio + coroutines)
// ------------------------------------------------------------

// Each client: stream the MBO slices once, then close send side.
awaitable<void> StreamMboToClient(tcp::socket socket, const DbnSlices& slices,
                                  const std::size_t max_batch_bytes,
                                  const std::size_t max_batch_records) {
  // Tune socket for high-throughput streaming. Errors here are non-fatal:
  // log and continue.
  boost::system::error_code ec;
  socket.set_option(asio::socket_base::send_buffer_size(1 << 20), ec);
  if (ec) {
    std::cerr << "[warning] send_buffer_size set_option failed: "
              << ec.message() << "\n";
    ec.clear();
  }

  socket.set_option(tcp::no_delay(true), ec);
  if (ec) {
    std::cerr << "[warning] tcp::no_delay set_option failed: " << ec.message()
              << "\n";
    ec.clear();
  }

  const std::size_t total_records = slices.size();
  std::size_t idx = 0;

  // Reuse buffer vector across batches to avoid repeated allocations.
  std::vector<asio::const_buffer> bufs;
  bufs.reserve(1024);

  // Walk the MBO slices in batches to reduce syscalls.
  while (idx < total_records) {
    bufs.clear();

    std::size_t batch_bytes = 0;
    std::size_t batch_records = 0;

    // Build up to max_batch_bytes or max_batch_records per batch.
    while (idx < total_records &&
           batch_bytes + slices[idx].size() <= max_batch_bytes &&
           batch_records < max_batch_records) {
      const auto& s = slices[idx++];
      bufs.emplace_back(static_cast<const void*>(s.data()), s.size());
      batch_bytes += s.size();
      ++batch_records;
    }

    if (bufs.empty()) {
      continue;
    }

    // Zero-copy: buffers point directly into the mmap'd file.
    const std::size_t n = co_await asio::async_write(
        socket, bufs, asio::redirect_error(use_awaitable, ec));

    if (ec || n == 0) {
      std::cerr << "[warning] async_write error: " << ec.message() << "\n";
      co_return;
    }
  }

  // Ignore shutdown errors.
  ec.clear();
  socket.shutdown(tcp::socket::shutdown_send, ec);
}

// Accept loop: each client gets its own coroutine, one full replay.
awaitable<void> AcceptLoop(asio::io_context& ctx, const tcp::endpoint& endpoint,
                           const DbnSlices& slices,
                           const std::size_t max_batch_bytes,
                           const std::size_t max_batch_records) {
  boost::system::error_code ec;

  tcp::acceptor acceptor(ctx);
  acceptor.open(endpoint.protocol(), ec);
  if (ec) {
    std::cerr << "[fatal] acceptor.open error: " << ec.message() << "\n";
    co_return;
  }

  acceptor.set_option(asio::socket_base::reuse_address(true), ec);
  if (ec) {
    std::cerr << "[fatal] set_option(reuse_address) error: " << ec.message()
              << "\n";
    co_return;
  }

  acceptor.bind(endpoint, ec);
  if (ec) {
    std::cerr << "[fatal] bind error: " << ec.message() << "\n";
    co_return;
  }

  acceptor.listen(asio::socket_base::max_listen_connections, ec);
  if (ec) {
    std::cerr << "[fatal] listen error: " << ec.message() << "\n";
    co_return;
  }

  std::cout << "[server] listening on " << endpoint << "\n";

  for (;;) {
    tcp::socket socket(ctx);
    co_await acceptor.async_accept(socket,
                                   asio::redirect_error(use_awaitable, ec));

    if (ec) {
      std::cerr << "[fatal] accept error: " << ec.message() << "\n";
      continue;
    }

    auto remote = socket.remote_endpoint(ec);
    if (!ec) {
      std::cout << "[server] accepted connection from " << remote << "\n";
    } else {
      std::cerr << "[warning] remote_endpoint error: " << ec.message() << "\n";
      ec.clear();
      continue;
    }

    co_spawn(ctx,
             StreamMboToClient(std::move(socket), slices, max_batch_bytes,
                               max_batch_records),
             detached);
  }
}

void RunServer(const ServerConfig& cfg, const DbnSlices& slices) {
  asio::io_context ctx;

  // Graceful shutdown on SIGINT/SIGTERM.
  asio::signal_set signals(ctx, SIGINT, SIGTERM);
  signals.async_wait([&](const boost::system::error_code&, int) {
    std::cout << "[server] signal received, stopping io_context\n";
    ctx.stop();
  });

  tcp::endpoint endpoint(tcp::v4(), cfg.port);

  co_spawn(ctx,
           AcceptLoop(ctx, endpoint, slices, cfg.max_batch_bytes,
                      cfg.max_batch_records),
           detached);

  const auto threads = std::max(std::thread::hardware_concurrency(), 2U);
  std::vector<std::jthread> workers;
  workers.reserve(threads);
  for (auto i = 0U; i < threads; ++i) {
    workers.emplace_back([&ctx]() { ctx.run(); });
  }
}

// ------------------------------------------------------------
// Entry point
// ------------------------------------------------------------

int main(int argc, char** argv) {
  const auto cfg = ParseArgs(argc, argv);
  if (!cfg)
    return EXIT_FAILURE;

  const MmapRegion region(cfg->dbn_path);
  if (!region.valid())
    return EXIT_FAILURE;

  const auto records = GetDbnFileView(region);
  if (!records)
    return EXIT_FAILURE;

  const auto slices = GetDbnSlices(*records);
  if (!slices || slices->empty())
    return EXIT_FAILURE;

  RunServer(*cfg, *slices);
  return EXIT_SUCCESS;
}

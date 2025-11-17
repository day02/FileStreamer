// file_stream_client.cpp
//
// Stream ONLY MBO (rtype 0xA0) records received from server to MBO.CSV
//
// Run:
//
//   ./file_stream_client data.csv 9000
//
#include <boost/asio.hpp>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace asio = boost::asio;
using asio::ip::tcp;

// ------------------------------------------------------------
// DBN header (must match server)
// MBO record layout (our in-memory view)
//
// The wire payload may be smaller than this struct.
// We only memcpy as many bytes as are present; rest stays zero.
// ------------------------------------------------------------
static constexpr std::uint8_t RTYPE_MBO = 0xA0;

#pragma pack(push, 1)
struct DbnRecordHeader {
  std::uint8_t length_words;
  std::uint8_t rtype;
  std::uint16_t publisher_id;
  std::uint32_t instrument_id;
};

struct MboRecord {
  DbnRecordHeader hdr;
  std::uint64_t ts_event;
  std::uint64_t order_id;
  std::uint64_t price;
  std::uint32_t size;
  std::uint8_t flags;
  std::uint8_t channel_id;
  char action;
  char side;
  std::uint64_t ts_recv;
  std::int32_t ts_in_delta;
  std::uint32_t sequence;
};
#pragma pack(pop)

// ------------------------------------------------------------
// Stats & config
// ------------------------------------------------------------

struct StreamStats {
  std::size_t total_bytes_read = 0;
  std::size_t total_records = 0;
  std::size_t non_mbo_records = 0;
  std::size_t mbo_records = 0;
};

struct ClientConfig {
  std::string output_path;
  std::uint16_t port = 0;
};

// ------------------------------------------------------------
// Argument parsing
// ------------------------------------------------------------

std::optional<std::uint16_t> ParsePort(const std::string& s) {
  int v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size() || v < 1 || v > 65535)
    return std::nullopt;
  return static_cast<std::uint16_t>(v);
}

std::optional<ClientConfig> ParseArgs(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <output.csv> <port>\n";
    return std::nullopt;
  }

  ClientConfig cfg;
  cfg.output_path = argv[1];

  auto port = ParsePort(argv[2]);
  if (!port) {
    std::cerr << "Invalid port: " << argv[2] << "\n";
    return std::nullopt;
  }
  cfg.port = *port;
  return cfg;
}

// ------------------------------------------------------------
// Helpers: formatting for CSV
// ------------------------------------------------------------

// Convert nanoseconds since UNIX epoch to
// "YYYY-MM-DDTHH:MM:SS.fffffffffZ" (UTC, 9-digit nanos).
static std::string FormatTimestampNs(std::uint64_t ns_since_epoch) {
  using namespace std::chrono;

  std::chrono::nanoseconds ns(ns_since_epoch);
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
  auto nanos_part = ns - secs;

  std::time_t tt = static_cast<std::time_t>(secs.count());
  std::tm tm{};
  gmtime_r(&tt, &tm);

  char date_buf[32];
  std::strftime(date_buf, sizeof(date_buf), "%Y-%m-%dT%H:%M:%S", &tm);

  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s.%09lldZ", date_buf,
                static_cast<long long>(nanos_part.count()));
  return std::string(buf);
}

// Convert scaled integer price (scale 1e-9) to "xx.xxxxxxxxx".
std::string FormatPrice(std::int64_t px_scaled) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(9)
      << static_cast<long double>(px_scaled) / 1'000'000'000.0L;
  return oss.str();
}

// ------------------------------------------------------------
// Helpers: socket reading
// ------------------------------------------------------------

std::pair<std::size_t, bool> ReadFromSocket(tcp::socket& socket,
                                            std::vector<std::byte>& readbuf,
                                            StreamStats& stats) {
  bool error_happened = false;
  boost::system::error_code ec;
  const std::size_t n =
      socket.read_some(asio::buffer(readbuf.data(), readbuf.size()), ec);

  if (ec) {
    // Normal end-of-stream.
    if (ec == asio::error::eof || ec == asio::error::connection_reset ||
        ec == asio::error::shut_down) {
      std::cout << "[client] end of stream: total_bytes_read="
                << stats.total_bytes_read << "\n";
    } else {
      // Actual error.
      std::cerr << "[read error] " << ec.message() << "\n";
      error_happened = true;
    }
    return {0, error_happened};
  }

  stats.total_bytes_read += n;
  return {n, error_happened};
}

// ------------------------------------------------------------
// Helpers: backlog handling & parsing
// ------------------------------------------------------------

void AppendToBacklog(std::vector<std::byte>& backlog,
                     const std::vector<std::byte>& readbuf,
                     const std::size_t n) {
  backlog.insert(backlog.end(), readbuf.begin(), std::next(readbuf.begin(), n));
}

// Parse a single record from backlog at offset `off`.
// Returns false if we can't parse a full record (need more bytes) or
// if we hit a fatal condition.
bool ParseSingleRecord(const std::vector<std::byte>& backlog,
                       const std::size_t total, std::size_t& offset,
                       std::vector<MboRecord>& out, StreamStats& stats) {
  const std::size_t remaining = total - offset;
  if (remaining < sizeof(DbnRecordHeader))
    return false;

  DbnRecordHeader hdr{};
  std::memcpy(&hdr, backlog.data() + offset, sizeof(hdr));

  constexpr std::size_t kWordBytes = 4u;
  const std::size_t words = static_cast<std::size_t>(hdr.length_words);
  const std::size_t rec_bytes = words * kWordBytes;

  if (rec_bytes == 0) {
    std::cerr << "[fatal] zero-length record encountered\n";
    return false;
  }

  if (remaining < rec_bytes)
    return false;  // not enough data yet

  // We have a full record.
  ++stats.total_records;

  // Non-MBO: skip but still count as a packet.
  if (hdr.rtype != RTYPE_MBO) {
    ++stats.non_mbo_records;
    offset += rec_bytes;
    return true;
  }

  const std::size_t payload_bytes = rec_bytes - sizeof(DbnRecordHeader);
  if (payload_bytes == 0) {
    ++stats.mbo_records;  // count but ignore payload
    offset += rec_bytes;
    return true;
  }

  out.push_back({});
  std::memcpy(&out.back(), backlog.data() + offset, sizeof(MboRecord));
  ++stats.mbo_records;

  offset += rec_bytes;
  return true;
}

// Parse as many full records as possible from backlog.
void ParseRecordsFromBacklog(std::vector<std::byte>& backlog,
                             std::vector<MboRecord>& out, StreamStats& stats) {
  std::size_t offset = 0;
  const std::size_t total = backlog.size();

  while (true) {
    const std::size_t remaining = total - offset;
    if (remaining == 0)
      break;

    const std::size_t prev_offset = offset;
    const bool ok = ParseSingleRecord(backlog, total, offset, out, stats);
    if (!ok)
      break;  // need more data or fatal condition

    if (offset <= prev_offset)
      break;  // safety
  }

  if (offset > 0) {
    backlog.erase(backlog.begin(),
                  backlog.begin() + static_cast<std::ptrdiff_t>(offset));
  }
}

// ------------------------------------------------------------
// Top-level streaming function
// ------------------------------------------------------------

std::vector<MboRecord> ReceiveMboStream(tcp::socket& socket,
                                        StreamStats& stats) {
  std::vector<MboRecord> out;
  out.reserve(1'000'000);

  std::vector<std::byte> readbuf(1 << 20);  // 1 MB
  std::vector<std::byte> backlog;
  backlog.reserve(1 << 20);

  while (true) {
    const auto [bytes_just_read, error_happened] =
        ReadFromSocket(socket, readbuf, stats);
    // EOF: parse any remaining backlog and then break.
    if (bytes_just_read == 0) {
      if (!backlog.empty()) {
        ParseRecordsFromBacklog(backlog, out, stats);
      }
      break;
    }

    if (bytes_just_read > 0) {
      AppendToBacklog(backlog, readbuf, bytes_just_read);
      ParseRecordsFromBacklog(backlog, out, stats);
    }

    if (error_happened)
      break;
  }

  return out;
}

// ------------------------------------------------------------
// CSV writer (Databento-style)
// ------------------------------------------------------------

void WriteCsvHeader(std::ofstream& ofs) {
  ofs << "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,"
         "price,size,channel_id,order_id,flags,ts_in_delta,sequence,symbol\n";
}

void WriteCsvRecord(std::ofstream& ofs, const MboRecord& r) {
  const std::string ts_recv_str = FormatTimestampNs(r.ts_recv);
  const std::string ts_event_str = FormatTimestampNs(r.ts_event);
  const std::string price_str = FormatPrice(r.price);

  ofs << ts_recv_str << ',' << ts_event_str << ','
      << static_cast<int>(r.hdr.rtype) << ',' << r.hdr.publisher_id << ','
      << r.hdr.instrument_id << ',' << r.action << ',' << r.side << ','
      << price_str << ',' << r.size << ',' << static_cast<int>(r.channel_id)
      << ',' << r.order_id << ',' << static_cast<int>(r.flags) << ','
      << r.ts_in_delta << ',' << r.sequence << ',' << "CLX5" << '\n';
}

void DumpCsv(const std::vector<MboRecord>& v, const std::string& path) {
  std::ofstream ofs(path);
  if (!ofs) {
    std::cerr << "[fatal] cannot open '" << path << "' for writing\n";
    std::exit(1);
  }

  WriteCsvHeader(ofs);
  for (const auto& r : v) {
    WriteCsvRecord(ofs, r);
  }

  std::cout << "[client] wrote " << v.size() << " MBO records to CSV: " << path
            << "\n";
}

// ------------------------------------------------------------
// Networking: connect helpers
// ------------------------------------------------------------

bool ConnectToServer(asio::io_context& ctx, tcp::socket& sock,
                     const std::string& host, std::uint16_t port) {
  tcp::resolver resolver(ctx);
  boost::system::error_code ec;

  auto endpoints = resolver.resolve(host, std::to_string(port), ec);
  if (ec) {
    std::cerr << "[resolve error] " << ec.message() << "\n";
    return false;
  }

  asio::connect(sock, endpoints, ec);
  if (ec) {
    std::cerr << "[connect error] " << ec.message() << "\n";
    return false;
  }

  std::cout << "[client] connected to " << host << ":" << port << "\n";
  return true;
}

// ------------------------------------------------------------
// Stats printing helpers
// ------------------------------------------------------------

void PrintStreamSummary(const StreamStats& stats, std::size_t num_mbo_records) {
  std::cerr << "[client] debug: total_bytes_read=" << stats.total_bytes_read
            << ", total_records=" << stats.total_records
            << ", non_mbo_records=" << stats.non_mbo_records
            << ", mbo_records=" << stats.mbo_records << "\n";

  std::cout << "[client] received " << num_mbo_records
            << " MBO records (parsed)\n";
}

void PrintPerformanceStats(const StreamStats& stats, double elapsed_sec) {
  if (elapsed_sec > 0.0) {
    // Convert to milliseconds
    const double elapsed_ms = elapsed_sec * 1000.0;

    const double packets_per_ms =
        static_cast<double>(stats.total_records) / elapsed_ms;
    const double mbo_per_ms =
        static_cast<double>(stats.mbo_records) / elapsed_ms;

    std::cout << "[client] time_taken_sec=" << elapsed_sec
              << ", total_packets=" << stats.total_records
              << ", total_mbo=" << stats.mbo_records << "\n";

    std::cout << "[client] packets_per_ms=" << packets_per_ms
              << ", mbo_per_ms=" << mbo_per_ms << "\n";
  } else {
    std::cout << "[client] time_taken_sec ~ 0 (too fast to measure)\n";
  }
}

// ------------------------------------------------------------
// High-level client runner
// ------------------------------------------------------------

void RunClient(const ClientConfig& cfg, tcp::socket& sock) {
  StreamStats stats{};

  const auto t0 = std::chrono::steady_clock::now();
  auto records = ReceiveMboStream(sock, stats);
  const auto t1 = std::chrono::steady_clock::now();

  const double elapsed_sec = std::chrono::duration<double>(t1 - t0).count();

  PrintStreamSummary(stats, records.size());
  PrintPerformanceStats(stats, elapsed_sec);

  DumpCsv(records, cfg.output_path);
}

// ------------------------------------------------------------
// Main — args: <output.csv> <port>
// ------------------------------------------------------------

int main(int argc, char** argv) {
  auto cfgOpt = ParseArgs(argc, argv);
  if (!cfgOpt) {
    return 1;
  }
  const ClientConfig cfg = *cfgOpt;

  const std::string host = "127.0.0.1";

  asio::io_context ctx;
  tcp::socket sock(ctx);

  if (!ConnectToServer(ctx, sock, host, cfg.port)) {
    return 1;
  }

  RunClient(cfg, sock);

  return 0;
}

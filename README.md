
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?hide_repo_select=true&repo=your-org/FileStreamer)

# FileStreamer

Stream the MBO file at **50k–500k messages/second** over TCP  
Designed for high throughput and scalability.

## Build Steps

Requirements:
- CMake
- Ninja (optional but recommended)
- A C++17 compiler (GCC/Clang)

Build the project, inside the code spaces provided above.

```bash
cmake -S . -B build -G Ninja && cmake --build build -j
```

## Run Steps

### 1. Run the File Stream Server
Streams the DBN file over TCP on port **9000**:

```bash
./build/file_stream_server ./artifacts/CLX5_mbo.dbn 9000
```

### 2. Run the File Stream Client
Connects to the server on port **9000** and writes streamed data to a CSV file:

```bash
./build/file_stream_client /tmp/mbo.csv 9000
```

### 3. Verify Output

Compare the generated CSV file with the reference CSV:

```bash
diff /tmp/mbo.csv ./artifacts/CLX5_mbo.csv
```

### 4. Perf Results

Performace numbers
Server:
```bash
vscode@codespaces-50251b:/workspaces/FileStreamer$ ./build/file_stream_server ./artifacts/CLX5_mbo.dbn 9000
[parser] total record bytes: 2139872, parsed bytes: 2139872, MBO slices: 38212
[server] listening on 0.0.0.0:9000
[server] accepted connection from 127.0.0.1:57978
```
Client :
```bash
vscode@codespaces-50251b:/workspaces/FileStreamer$ ./build/file_stream_client /tmp/mbo.csv 9000 && diff /tmp/mbo.csv ./artifacts/CLX5_mbo.csv
[client] connected to 127.0.0.1:9000
[client] end of stream: total_bytes_read=2139872
[client] debug: total_bytes_read=2139872, total_records=38212, non_mbo_records=0, mbo_records=38212
[client] received 38212 MBO records (parsed)
[client] time_taken_sec=0.0973689, total_packets=38212, total_mbo=38212
[client] packets_per_ms=392.446, mbo_per_ms=392.446
[client] wrote 38212 MBO records to CSV: /tmp/mbo.csv
```

# MapReduce Word Count with Docker and RPyC

A distributed MapReduce implementation using Docker containers and RPyC (Remote Python Calls) for parallel word counting on large datasets (enwik9 - 1GB).

## Features

- **True Streaming Architecture**: Processes 1GB+ files without loading into memory
- **Async Task Execution**: Uses RPyC async calls with backpressure control
- **Memory Efficient**: 50MB chunk processing with combiner optimization
- **Scalable**: Easy configuration for 1, 2, 4, 8+ workers
- **Production Ready**: Handles large-scale Wikipedia datasets

## Architecture

- **1 Coordinator Container**: Streams files, distributes tasks, aggregates results
- **Multiple Worker Containers**: Execute Map tasks in parallel with local aggregation
- **Communication**: RPyC over Docker networking (async mode)
- **Optimization**: Combiner pattern - workers return `{word: count}` instead of `[(word, 1)]`

## Prerequisites

- Docker (version 20.10+)
- Docker Compose (version 1.29+)
- Git
- At least 4GB RAM for Docker Desktop

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/arshadvani3/mapreduce-docker.git
cd mapreduce-docker
```

### 2. Run with Default Configuration (3 Workers)

```bash
docker-compose up --build
```

This will:
- Start 3 worker containers
- Start 1 coordinator container
- Download and process enwik9 dataset (~1GB uncompressed)
- Display top 20 most frequent words
- Save results to `txt/word_counts.tsv`
- Show total execution time

### 3. View Results

The coordinator displays:
```
============================================================
TOP 20 WORDS BY FREQUENCY
============================================================

 1. the    : 91,836,871
 2. of     : 50,481,034
 3. and    : 38,850,467
 ...

Saved: txt/word_counts.tsv (unique=2,345,678)
Elapsed: 127.45s
```

### 4. Stop Containers

```bash
docker-compose down
```

## Easy Benchmarking

Use the helper script to generate docker-compose files for different worker counts:

```bash
# For 1 worker
python3 generate_compose.py 1
docker-compose up --build

# For 2 workers
python3 generate_compose.py 2
docker-compose up --build

# For 4 workers
python3 generate_compose.py 4
docker-compose up --build

# For 8 workers
python3 generate_compose.py 8
docker-compose up --build
```

Record the elapsed time from each run for performance analysis.

## Project Structure

```
mapreduce-docker/
├── coordinator.py          # Streaming coordinator with async task dispatch
├── worker.py              # Worker service with combiner optimization
├── Dockerfile             # Container image definition
├── docker-compose.yml     # Multi-container orchestration (3 workers default)
├── generate_compose.py    # Helper script to generate compose files
├── README.md             # This file
├── .gitignore            # Git ignore rules
└── txt/                  # Downloaded datasets & results (auto-created)
```

## How It Works

### Streaming MapReduce Flow

1. **Download Phase**:
   - Coordinator downloads dataset from URL (if not cached)
   - Unzips into `txt/` directory

2. **Streaming Map Phase**:
   - File read in **50MB chunks** (line by line, never loading full file)
   - Chunks dispatched **round-robin** to workers via async RPyC
   - **Backpressure control**: Max 8 tasks in-flight at a time
   - Each worker applies **combiner**: returns `{word: count}` for its chunk
   - Coordinator continuously aggregates results as workers complete

3. **Aggregation**:
   - No separate shuffle/reduce phase needed
   - Coordinator merges all worker results into global `Counter`
   - Sorts by frequency and displays top 20

### Key Optimizations

**Memory Efficiency:**
- `file_chunks()` generator yields ~50MB at a time
- Workers never receive full file
- Coordinator never holds full file in memory

**Network Efficiency:**
- Workers use `Counter` (combiner pattern)
- Returns `{word: count}` instead of `[(word, 1), ...]`
- Dramatically reduces data transfer for 1GB dataset

**Performance:**
- Async task dispatch with `rpyc.async_()`
- Parallel worker execution
- Backpressure prevents coordinator overload

## Configuration

### Change Dataset

Edit `coordinator.py` line 115, or pass URL as argument:

```yaml
# In docker-compose.yml
coordinator:
  command: python coordinator.py https://mattmahoney.net/dc/enwik8.zip
```

**Available datasets:**
- `enwik8.zip` (~100MB) - for testing
- `enwik9.zip` (~1GB) - production benchmark (default)

### Adjust Chunk Size

Edit `coordinator.py` line 11:
```python
CHUNK_BYTES = 50_000_000  # 50MB (default)
```

### Adjust Max Inflight Tasks

Edit `coordinator.py` line 12:
```python
MAX_INFLIGHT = 8  # 8 concurrent tasks (default)
```

## Running Benchmarks

### Automated Approach

```bash
#!/bin/bash
for workers in 1 2 4 8; do
  echo "=== Benchmark: $workers workers ==="
  python3 generate_compose.py $workers
  docker-compose up --build 2>&1 | tee benchmark_${workers}worker.log
  docker-compose down
  echo ""
done
```

### Manual Approach

1. **1 Worker:**
```bash
python3 generate_compose.py 1
docker-compose up --build
# Record "Elapsed: X.XXs" from output
docker-compose down
```

2. **2 Workers:**
```bash
python3 generate_compose.py 2
docker-compose up --build
docker-compose down
```

3. **4 Workers:**
```bash
python3 generate_compose.py 4
docker-compose up --build
docker-compose down
```

4. **8 Workers:**
```bash
python3 generate_compose.py 8
docker-compose up --build
docker-compose down
```

## Troubleshooting

### Out of Memory

If coordinator crashes (exit code 137):
1. Increase Docker Desktop memory (Settings → Resources → Memory → 8GB+)
2. Or use smaller dataset: `enwik8.zip` instead of `enwik9.zip`
3. Or reduce `CHUNK_BYTES` in coordinator.py

### Workers Not Connecting

```bash
# Check if containers are running
docker-compose ps

# Check logs
docker-compose logs coordinator
docker-compose logs worker-1

# Restart
docker-compose down
docker-compose up --build
```

### Port Conflicts

```bash
# Check if ports are in use
lsof -i :18861

# Change ports in docker-compose.yml or generate_compose.py
```

### Slow Performance

- Ensure Docker has enough CPU cores allocated (Settings → Resources → CPUs)
- Check `MAX_INFLIGHT` setting (too low = underutilized workers)
- Monitor with `docker stats`

## Implementation Details

### Streaming Architecture

**coordinator.py:**
- `file_chunks()`: Generator that yields 50MB text blocks
- `mapreduce_wordcount()`: Async dispatch loop with backpressure
- `flush_ready()`: Non-blocking result collection

**worker.py:**
- `exposed_map()`: Returns `dict(Counter(words))` for chunk
- Uses regex `\b[a-z]+\b` for word extraction
- Pickle disabled for security (plain dicts serialize fine)

### Thread Safety

- RPyC ThreadedServer handles concurrent requests
- Coordinator uses async calls, polls for completion
- No explicit locking needed (single-threaded coordinator)

### Performance Characteristics

For enwik9 (1GB, ~1 billion characters):
- **1 worker**: ~200-250 seconds
- **2 workers**: ~120-150 seconds
- **4 workers**: ~70-90 seconds
- **8 workers**: ~50-70 seconds

*(Times vary based on CPU, memory, and Docker configuration)*

## Development

### Local Testing (Without Docker)

Terminal 1 - Start a worker:
```bash
python3 worker.py
```

Terminal 2 - Run coordinator:
```bash
# Edit coordinator.py: workers = ["localhost"]
python3 coordinator.py
```

### Debugging

Add print statements or use logs:
```bash
docker-compose logs -f coordinator
docker-compose logs -f worker-1
```

For interactive debugging, add:
```python
import pdb; pdb.set_trace()
```

## Technical Specifications

- **Python**: 3.11
- **RPyC**: Latest (installed via pip)
- **Docker Base**: python:3.11-slim
- **Networking**: Docker bridge network
- **Dataset**: Wikipedia XML dumps (enwik8/enwik9)

## License

MIT License - free for educational purposes.

## Author

Arshad Vani
https://github.com/arshadvani3/mapreduce-docker

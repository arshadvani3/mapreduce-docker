# MapReduce Word Count with Docker and RPyC

A distributed MapReduce implementation using Docker containers and RPyC (Remote Python Calls) for parallel word counting on large datasets.

## Architecture

- **1 Coordinator Container**: Orchestrates MapReduce workflow, handles task distribution and failure detection
- **Multiple Worker Containers**: Execute Map and Reduce tasks in parallel
- **Communication**: RPyC over Docker networking
- **Fault Tolerance**: 20-second timeout with automatic task reassignment

## Prerequisites

- Docker (version 20.10+)
- Docker Compose (version 1.29+)
- Git

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/mapreduce-docker.git
cd mapreduce-docker
```

### 2. Build Docker Images

```bash
docker-compose build
```

This builds a Docker image containing:
- Python 3.11
- RPyC library
- coordinator.py and worker.py

### 3. Run with Default Configuration (3 Workers)

```bash
docker-compose up
```

This will:
- Start 3 worker containers
- Start 1 coordinator container
- Download and process enwik8 dataset (~100MB uncompressed)
- Display top 20 most frequent words
- Show total execution time

### 4. View Logs

In another terminal:
```bash
# View all logs
docker-compose logs -f

# View specific container
docker-compose logs -f coordinator
docker-compose logs -f worker-1
```

### 5. Stop Containers

```bash
docker-compose down
```

## Configuration

### Change Number of Workers

Edit `docker-compose.yml`:

**For 1 Worker:**
```yaml
environment:
  - NUM_WORKERS=1

# Keep only worker-1, comment out worker-2 and worker-3
```

**For 2 Workers:**
```yaml
environment:
  - NUM_WORKERS=2

# Keep worker-1 and worker-2, comment out worker-3
```

**For 4 Workers:**
```yaml
environment:
  - NUM_WORKERS=4

# Add worker-4:
  worker-4:
    build: .
    container_name: worker-4
    hostname: worker-4
    command: python worker.py
    networks:
      - mapreduce-network
    ports:
      - "18864:18861"
```

**For 8 Workers:** Add worker-4 through worker-8 following the same pattern.

### Change Dataset

The coordinator accepts a URL as an argument. Edit `docker-compose.yml`:

```yaml
coordinator:
  # ... other config ...
  command: python coordinator.py https://mattmahoney.net/dc/enwik9.zip
```

**Available datasets:**
- `enwik8.zip` (~100MB uncompressed) - good for testing
- `enwik9.zip` (~1GB uncompressed) - full benchmark

## Project Structure

```
mapreduce-docker/
├── coordinator.py          # Coordinator logic (task distribution, failure handling)
├── worker.py              # Worker service (map and reduce functions)
├── Dockerfile             # Container image definition
├── docker-compose.yml     # Multi-container orchestration
├── README.md             # This file
├── .gitignore            # Git ignore rules
└── txt/                  # Downloaded datasets (created automatically)
```

## How It Works

### MapReduce Flow

1. **Download Phase**: Coordinator downloads dataset from URL
2. **Map Phase**: 
   - Text split into N chunks (N = number of workers)
   - Each worker tokenizes and counts words in parallel
   - Returns intermediate pairs: `[(word, 1), (word, 1), ...]`
3. **Shuffle Phase**:
   - Coordinator groups all intermediate pairs by word
   - Creates: `{word: [1, 1, 1, ...], ...}`
4. **Reduce Phase**:
   - Words partitioned across workers
   - Each worker sums counts for its partition
   - Runs in parallel
5. **Aggregation Phase**:
   - Coordinator combines all results
   - Sorts by frequency
   - Displays top 20

### Fault Tolerance

- Each task has a 20-second timeout
- If worker fails or times out, coordinator reassigns the task
- Uses thread-safe TaskTracker for concurrent task management

### Communication

Workers expose RPyC services on port 18861:
- `exposed_map(text_chunk)` - tokenize and count
- `exposed_reduce(grouped_items)` - sum counts

Coordinator connects via hostnames defined in docker-compose:
- `worker-1`, `worker-2`, `worker-3`, etc.

## Running Benchmarks

### Test with 1, 2, 4, 8 Workers

1. **1 Worker:**
```bash
# Edit docker-compose.yml: NUM_WORKERS=1, keep only worker-1
docker-compose up --build
# Record time
docker-compose down
```

2. **2 Workers:**
```bash
# Edit docker-compose.yml: NUM_WORKERS=2, keep worker-1 and worker-2
docker-compose up --build
# Record time
docker-compose down
```

3. **4 Workers:**
```bash
# Edit docker-compose.yml: NUM_WORKERS=4, add worker-4
docker-compose up --build
# Record time
docker-compose down
```

4. **8 Workers:**
```bash
# Edit docker-compose.yml: NUM_WORKERS=8, add workers 5-8
docker-compose up --build
# Record time
docker-compose down
```

## Troubleshooting

### Workers not connecting
```bash
# Check if containers are running
docker-compose ps

# Check network
docker network ls
docker network inspect mapreduce-docker_mapreduce-network

# Restart
docker-compose down
docker-compose up --build
```

### Port conflicts
```bash
# Check if ports are in use
lsof -i :18861

# Change ports in docker-compose.yml
```

### Out of memory
```bash
# Use smaller dataset (enwik8 instead of enwik9)
# Or reduce number of workers
```

### Permission denied on txt/
```bash
# Fix permissions
chmod -R 755 txt/
```

## Development

### Local Testing (Without Docker)

Terminal 1 - Start a worker:
```bash
python worker.py
```

Terminal 2 - Run coordinator:
```bash
# Edit coordinator.py: WORKERS = [("localhost", 18861)]
python coordinator.py
```

### Debugging

Add print statements or use Python debugger:
```python
import pdb; pdb.set_trace()
```

View container logs:
```bash
docker logs -f coordinator
docker logs -f worker-1
```

## Implementation Details

### Thread Safety
- `TaskTracker` uses locks for concurrent access
- Map and Reduce tasks run in separate threads
- Shared result dictionaries protected by thread synchronization

### Performance Optimizations
- Parallel execution of Map and Reduce tasks
- Efficient text chunking
- Minimized data transfer via RPyC

### Limitations
- Assumes UTF-8 text files only
- Single coordinator (no coordinator fault tolerance)
- Workers must be reachable via Docker networking

## License

MIT License - feel free to use for educational purposes.

## Authors

[Your Name]
CSE 239 - Advanced Cloud Computing

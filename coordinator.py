import rpyc
import sys
import time
import glob
import urllib.request
import zipfile
import os
import collections
import threading
from typing import List, Dict, Tuple

# Configure worker connections
# These hostnames are defined in docker-compose.yml
WORKERS = []  # Will be populated based on NUM_WORKERS environment variable


class TaskTracker:
    """Thread-safe task tracking with failure detection"""
    
    def __init__(self, timeout=20):
        self.timeout = timeout
        self.lock = threading.Lock()
        self.tasks = {}  # task_id -> {'status': 'pending/running/complete', 'start_time': time, 'worker': worker_addr}
        
    def assign_task(self, task_id, worker_addr):
        """Mark task as assigned to a worker"""
        with self.lock:
            self.tasks[task_id] = {
                'status': 'running',
                'start_time': time.time(),
                'worker': worker_addr
            }
    
    def complete_task(self, task_id):
        """Mark task as complete"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'complete'
    
    def get_failed_tasks(self):
        """Return list of task_ids that exceeded timeout"""
        failed = []
        current_time = time.time()
        with self.lock:
            for task_id, info in self.tasks.items():
                if info['status'] == 'running':
                    if current_time - info['start_time'] > self.timeout:
                        failed.append(task_id)
                        print(f"[COORDINATOR] Task {task_id} failed (timeout)! Will reassign.")
        return failed


def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    """
    Downloads and unzips a wikipedia dataset into txt/ directory.
    Skips if files already exist.
    
    Args:
        url: URL to download dataset from
        
    Returns:
        List of file paths in txt/ directory
    """
    os.makedirs('txt', exist_ok=True)
    
    filename = url.split('/')[-1]
    filepath = filename
    
    # Check if already downloaded
    if not os.path.exists(filepath):
        print(f"[COORDINATOR] Downloading {url}...")
        urllib.request.urlretrieve(url, filepath)
        print(f"[COORDINATOR] Download complete!")
    else:
        print(f"[COORDINATOR] Dataset {filename} already exists, skipping download.")
    
    # Unzip if needed
    txt_files = glob.glob('txt/*')
    if not txt_files:
        print(f"[COORDINATOR] Unzipping {filename}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall('txt/')
        print(f"[COORDINATOR] Unzip complete!")
    else:
        print(f"[COORDINATOR] Files already exist in txt/, skipping unzip.")
    
    txt_files = glob.glob('txt/*')
    print(f"[COORDINATOR] Found {len(txt_files)} files to process")
    return txt_files


def split_text(text, n):
    """
    Split text into n roughly equal chunks.
    
    Args:
        text: String to split
        n: Number of chunks
        
    Returns:
        List of text chunks
    """
    chunk_size = len(text) // n
    chunks = []
    
    for i in range(n):
        start = i * chunk_size
        # Last chunk gets any remainder
        end = start + chunk_size if i < n - 1 else len(text)
        chunks.append(text[start:end])
    
    return chunks


def partition_keys(keys, n):
    """
    Partition keys into n groups for reduce workers.
    
    Args:
        keys: List of keys (words)
        n: Number of partitions
        
    Returns:
        List of key lists
    """
    keys = list(keys)
    keys.sort()  # Sort for consistent partitioning
    
    chunk_size = len(keys) // n
    partitions = []
    
    for i in range(n):
        start = i * chunk_size
        end = start + chunk_size if i < n - 1 else len(keys)
        partitions.append(keys[start:end])
    
    return partitions


def connect_to_worker(worker_addr, timeout=5):
    """
    Connect to a worker with timeout.
    
    Args:
        worker_addr: Tuple of (hostname, port)
        timeout: Connection timeout in seconds
        
    Returns:
        RPyC connection or None if failed
    """
    try:
        conn = rpyc.connect(
            worker_addr[0],
            worker_addr[1],
            config={
                'allow_public_attrs': True,
                'allow_pickle': True,
                'sync_request_timeout': 30
            }
        )
        return conn
    except Exception as e:
        print(f"[COORDINATOR] Failed to connect to {worker_addr}: {e}")
        return None


def execute_map_task(worker_addr, task_id, text_chunk, results, task_tracker):
    """
    Execute a map task on a worker (runs in separate thread).
    
    Args:
        worker_addr: Tuple of (hostname, port)
        task_id: Unique task identifier
        text_chunk: Text to process
        results: Shared dict to store results {task_id: result}
        task_tracker: TaskTracker instance
    """
    try:
        task_tracker.assign_task(task_id, worker_addr)
        
        conn = connect_to_worker(worker_addr)
        if conn is None:
            print(f"[COORDINATOR] Map task {task_id} failed - couldn't connect to worker")
            return
        
        print(f"[COORDINATOR] Starting map task {task_id} on {worker_addr[0]}")
        result = conn.root.exposed_map(text_chunk)
        
        # Force complete data transfer by converting to local list before connection closes
        local_result = []
        for item in result:
            local_result.append(tuple(item))  # Convert each item to tuple
        
        results[task_id] = local_result
        task_tracker.complete_task(task_id)
        print(f"[COORDINATOR] Map task {task_id} completed successfully")
        
        conn.close()
    except Exception as e:
        print(f"[COORDINATOR] Map task {task_id} failed with error: {e}")


def execute_reduce_task(worker_addr, task_id, grouped_items, results, task_tracker):
    """
    Execute a reduce task on a worker (runs in separate thread).
    
    Args:
        worker_addr: Tuple of (hostname, port)
        task_id: Unique task identifier
        grouped_items: Dict of {word: [counts]}
        results: Shared dict to store results {task_id: result}
        task_tracker: TaskTracker instance
    """
    try:
        task_tracker.assign_task(task_id, worker_addr)
        
        conn = connect_to_worker(worker_addr)
        if conn is None:
            print(f"[COORDINATOR] Reduce task {task_id} failed - couldn't connect to worker")
            return
        
        print(f"[COORDINATOR] Starting reduce task {task_id} on {worker_addr[0]}")
        result = conn.root.exposed_reduce(grouped_items)
        
        # Force complete data transfer by converting to local dict before connection closes
        local_result = dict(result)
        
        results[task_id] = local_result
        task_tracker.complete_task(task_id)
        print(f"[COORDINATOR] Reduce task {task_id} completed successfully")
        
        conn.close()
    except Exception as e:
        print(f"[COORDINATOR] Reduce task {task_id} failed with error: {e}")


def mapreduce_wordcount(input_files):
    """
    Execute MapReduce word count across distributed workers.
    
    Args:
        input_files: List of file paths to process
        
    Returns:
        List of (word, count) tuples sorted by count (descending)
    """
    print(f"\n[COORDINATOR] Starting MapReduce with {len(WORKERS)} workers")
    
    # Read all input files
    print(f"[COORDINATOR] Reading {len(input_files)} input files...")
    all_text = ""
    for filepath in input_files:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            all_text += f.read()
    
    print(f"[COORDINATOR] Total text size: {len(all_text)} characters")
    
    # ============= MAP PHASE =============
    print(f"\n[COORDINATOR] === MAP PHASE ===")
    
    # Split text into chunks (one per worker)
    num_map_workers = len(WORKERS)
    text_chunks = split_text(all_text, num_map_workers)
    print(f"[COORDINATOR] Split text into {len(text_chunks)} chunks")
    
    # Execute map tasks in parallel
    map_results = {}
    task_tracker = TaskTracker(timeout=20)
    threads = []
    
    for i, (worker_addr, chunk) in enumerate(zip(WORKERS, text_chunks)):
        task_id = f"map_{i}"
        thread = threading.Thread(
            target=execute_map_task,
            args=(worker_addr, task_id, chunk, map_results, task_tracker)
        )
        thread.start()
        threads.append(thread)
    
    # Wait for all map tasks to complete
    for thread in threads:
        thread.join()
    
    # Check for failed tasks and reassign
    failed_tasks = task_tracker.get_failed_tasks()
    while failed_tasks:
        print(f"[COORDINATOR] Reassigning {len(failed_tasks)} failed map tasks...")
        for task_id in failed_tasks:
            # Find an available worker (simple round-robin)
            worker_idx = int(task_id.split('_')[1]) % len(WORKERS)
            worker_addr = WORKERS[worker_idx]
            chunk = text_chunks[worker_idx]
            
            thread = threading.Thread(
                target=execute_map_task,
                args=(worker_addr, task_id, chunk, map_results, task_tracker)
            )
            thread.start()
            thread.join()  # Wait for retry
        
        failed_tasks = task_tracker.get_failed_tasks()
    
    print(f"[COORDINATOR] Map phase complete! Collected {len(map_results)} results")
    
    # ============= SHUFFLE PHASE =============
    print(f"\n[COORDINATOR] === SHUFFLE PHASE ===")
    
    # Combine all intermediate pairs and group by key
    intermediate_data = collections.defaultdict(list)
    
    for task_id, pairs in map_results.items():
        # Data is already local from execute_map_task
        for word, count in pairs:
            intermediate_data[word].append(count)
    
    print(f"[COORDINATOR] Grouped {len(intermediate_data)} unique words")
    
    # ============= REDUCE PHASE =============
    print(f"\n[COORDINATOR] === REDUCE PHASE ===")
    
    # Partition keys for reduce workers
    num_reduce_workers = len(WORKERS)
    key_partitions = partition_keys(intermediate_data.keys(), num_reduce_workers)
    
    # Execute reduce tasks in parallel
    reduce_results = {}
    task_tracker = TaskTracker(timeout=20)
    threads = []
    
    for i, (worker_addr, keys) in enumerate(zip(WORKERS, key_partitions)):
        task_id = f"reduce_{i}"
        
        # Prepare data for this reducer
        grouped_items = {key: intermediate_data[key] for key in keys}
        
        thread = threading.Thread(
            target=execute_reduce_task,
            args=(worker_addr, task_id, grouped_items, reduce_results, task_tracker)
        )
        thread.start()
        threads.append(thread)
    
    # Wait for all reduce tasks
    for thread in threads:
        thread.join()
    
    # Check for failed reduce tasks and reassign
    failed_tasks = task_tracker.get_failed_tasks()
    while failed_tasks:
        print(f"[COORDINATOR] Reassigning {len(failed_tasks)} failed reduce tasks...")
        for task_id in failed_tasks:
            worker_idx = int(task_id.split('_')[1]) % len(WORKERS)
            worker_addr = WORKERS[worker_idx]
            keys = key_partitions[worker_idx]
            grouped_items = {key: intermediate_data[key] for key in keys}
            
            thread = threading.Thread(
                target=execute_reduce_task,
                args=(worker_addr, task_id, grouped_items, reduce_results, task_tracker)
            )
            thread.start()
            thread.join()
        
        failed_tasks = task_tracker.get_failed_tasks()
    
    print(f"[COORDINATOR] Reduce phase complete! Collected {len(reduce_results)} results")
    
    # ============= FINAL AGGREGATION =============
    print(f"\n[COORDINATOR] === AGGREGATION PHASE ===")
    
    # Combine all reduce results (thread-safe since sequential here)
    final_counts = {}
    for task_id, word_counts in reduce_results.items():
        final_counts.update(word_counts)
    
    print(f"[COORDINATOR] Total unique words: {len(final_counts)}")
    
    # Sort by count (descending)
    sorted_counts = sorted(final_counts.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_counts


if __name__ == "__main__":
    # Setup workers from environment variable
    num_workers = int(os.environ.get('NUM_WORKERS', 3))
    
    for i in range(1, num_workers + 1):
        WORKERS.append((f"worker-{i}", 18861))
    
    print(f"[COORDINATOR] Configured with {len(WORKERS)} workers: {WORKERS}")
    
    # Wait for workers to start
    print("[COORDINATOR] Waiting 5 seconds for workers to start...")
    time.sleep(5)
    
    # Download dataset
    url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik9.zip'
    input_files = download(url)
    
    # Run MapReduce
    start_time = time.time()
    word_counts = mapreduce_wordcount(input_files)
    end_time = time.time()
    
    # Display results
    print('\n' + '='*60)
    print('TOP 20 WORDS BY FREQUENCY')
    print('='*60 + '\n')
    
    top20 = word_counts[:20]
    longest = max(len(word) for word, count in top20)
    
    for i, (word, count) in enumerate(top20, 1):
        print(f'{i:2d}. {word:{longest+1}s}: {count:,}')
    
    elapsed_time = end_time - start_time
    print(f'\n{"="*60}')
    print(f'Elapsed Time: {elapsed_time:.2f} seconds')
    print(f'{"="*60}\n')

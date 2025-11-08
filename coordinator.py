import os
import sys
import time
import glob
import urllib.request
import zipfile
from collections import Counter
import rpyc


CHUNK_BYTES = 50_000_000
MAX_INFLIGHT = 8
WORKER_PORT = 18861

# worker list from env
def build_workers():
    n = int(os.environ.get("NUM_WORKERS", "3"))
    return [f"worker-{i}" for i in range(1, n + 1)]

def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    os.makedirs('txt', exist_ok=True)
    filename = url.split('/')[-1]
    zip_path = filename

    if not os.path.exists(zip_path):
        print(f"[COORD] Downloading {url} ...")
        urllib.request.urlretrieve(url, zip_path)
        print("[COORD] Download complete.")
    else:
        print(f"[COORD] Found {zip_path}, skip download.")

    # If txt/ has nothing, unzip
    existing = [p for p in glob.glob('txt/*') if os.path.isfile(p)]
    if not existing:
        print(f"[COORD] Unzipping {zip_path} ...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall('txt/')
        print("[COORD] Unzip complete.")
    else:
        print("[COORD] txt/ already populated, skip unzip.")

    files = sorted(p for p in glob.glob('txt/*') if os.path.isfile(p))
    print(f"[COORD] Found {len(files)} files.")
    return files


def file_chunks(path, chunk_bytes=CHUNK_BYTES, encoding="utf-8"):
    """Yield ~chunk_bytes worth of text without loading whole file."""
    with open(path, "r", encoding=encoding, errors="ignore") as f:
        buf = []
        size = 0
        for line in f:
            buf.append(line)
            size += len(line.encode(encoding, "ignore"))
            if size >= chunk_bytes:
                yield "".join(buf)
                buf.clear()
                size = 0
        if buf:
            yield "".join(buf)

def mapreduce_wordcount(input_files, workers):
    print(f"[COORD] Starting MapReduce with workers={workers}")
    # connect once and reuse
    conns = [rpyc.connect(h, WORKER_PORT,
                          config={"allow_public_attrs": True, "allow_pickle": False,
                                  "sync_request_timeout": 60})
             for h in workers]
    rr_idx = 0

    global_counts = Counter()
    inflight = []  # list of (async_result)

    def flush_ready():
        nonlocal inflight, global_counts
        still = []
        for ar in inflight:
            if ar.ready:
                res = ar.value  # dict[str,int]
                global_counts.update(res)
            else:
                still.append(ar)
        inflight = still

    # stream all files, dispatch chunks round-robin with a cap on inflight
    dispatched = 0
    for path in input_files:
        print(f"[COORD] Processing {path}")
        for chunk in file_chunks(path):
            # backpressure
            while len(inflight) >= MAX_INFLIGHT:
                flush_ready()
                time.sleep(0.05)

            conn = conns[rr_idx]
            rr_idx = (rr_idx + 1) % len(conns)
            ar = rpyc.async_(conn.root.exposed_map)(chunk)  # FIXED: explicit exposed_map
            inflight.append(ar)
            dispatched += 1

    # drain remaining
    while inflight:
        flush_ready()
        time.sleep(0.05)

    print(f"[COORD] Mapped {dispatched} chunks; unique words={len(global_counts)}")
    return global_counts

if __name__ == "__main__":
    workers = build_workers()
    print(f"[COORD] Workers: {workers}")
    print("[COORD] Waiting 5s for workers...")
    time.sleep(5)

    url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik9.zip'
    files = download(url)

    t0 = time.time()
    counts = mapreduce_wordcount(files, workers)
    t1 = time.time()

    # Output
    print("\n" + "="*60)
    print("TOP 20 WORDS BY FREQUENCY")
    print("="*60 + "\n")
    top20 = counts.most_common(20)
    longest = max(len(w) for w, _ in top20) if top20 else 5
    for i, (w, c) in enumerate(top20, 1):
        print(f"{i:2d}. {w:{longest+1}s}: {c:,}")

    out_path = "txt/word_counts.tsv"
    with open(out_path, "w", encoding="utf-8") as f:
        for w, c in counts.most_common():
            f.write(f"{w}\t{c}\n")

    print(f"\nSaved: {out_path} (unique={len(counts)})")
    print(f"Elapsed: {t1 - t0:.2f}s")

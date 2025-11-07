import rpyc
import re
from collections import Counter
from rpyc.utils.server import ThreadedServer

WORD_RE = re.compile(r'\b[a-z]+\b')

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk: str):
        """
        Map step with combiner: returns {word: count} for this chunk.
        """
        words = WORD_RE.findall(text_chunk.lower())
        # Counter -> dict to keep it plain and small over the wire
        local_counts = dict(Counter(words))
        print(f"[WORKER] Mapped {len(words)} tokens -> {len(local_counts)} unique")
        return local_counts

    def exposed_reduce(self, grouped_items: dict[str, list[int]]):
        """
        (Optional) Real 'reduce' if you still shuffle by key.
        """
        return {w: sum(counts) for w, counts in grouped_items.items()}

if __name__ == "__main__":
    print("[WORKER] Starting on 0.0.0.0:18861 ...")
    server = ThreadedServer(
        MapReduceService,
        hostname="0.0.0.0",
        port=18861,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle": False,  # plain dicts serialize fine without pickle
        },
    )
    server.start()

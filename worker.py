import rpyc
import re
import collections
from rpyc.utils.server import ThreadedServer

class MapReduceService(rpyc.Service):
    """
    RPyC Service that exposes map and reduce operations.
    Workers run this service and wait for coordinator to call these methods.
    """
    
    def exposed_map(self, text_chunk):
        """
        Map step: tokenize and count words in text chunk.
        
        Args:
            text_chunk: String of text to process
            
        Returns:
            List of (word, 1) tuples - intermediate key-value pairs
        """
        print(f"[WORKER] Received map task with {len(text_chunk)} characters")
        
        # Convert to lowercase and extract words (alphanumeric only)
        words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
        
        # Create intermediate pairs: each word gets a count of 1
        # This is the classic MapReduce pattern
        # IMPORTANT: Convert to list explicitly for RPyC serialization
        intermediate_pairs = list([(word, 1) for word in words])
        
        print(f"[WORKER] Map task complete: {len(intermediate_pairs)} word occurrences")
        return intermediate_pairs
    
    def exposed_reduce(self, grouped_items):
        """
        Reduce step: sum counts for a subset of words.
        
        Args:
            grouped_items: Dictionary like {'word': [1, 1, 1, ...]}
            
        Returns:
            Dictionary like {'word': total_count}
        """
        print(f"[WORKER] Received reduce task with {len(grouped_items)} unique words")
        
        # Sum up all the counts for each word
        results = {}
        for word, counts in grouped_items.items():
            results[word] = sum(counts)
        
        print(f"[WORKER] Reduce task complete")
        return results


if __name__ == "__main__":
    """
    Start the RPyC server on port 18861.
    The coordinator will connect to this port to send tasks.
    """
    print("[WORKER] Starting MapReduce worker service on port 18861...")
    
    # ThreadedServer allows handling multiple requests concurrently
    server = ThreadedServer(
        MapReduceService,
        port=18861,
        protocol_config={
            'allow_public_attrs': True,
            'allow_pickle': True,  # Needed to transfer complex Python objects
        }
    )
    
    print("[WORKER] Worker is ready and waiting for tasks!")
    server.start()

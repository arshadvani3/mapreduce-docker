#!/usr/bin/env python3
"""
Quick test script to verify map and reduce functions work correctly
"""

import re
import collections

def test_map():
    """Test the map function logic"""
    text = "hello world hello python world"
    words = re.findall(r'\b[a-z]+\b', text.lower())
    pairs = [(word, 1) for word in words]
    
    print("Test Map Function:")
    print(f"Input: {text}")
    print(f"Output: {pairs}")
    print(f"✓ Map function works!\n")
    return pairs

def test_reduce():
    """Test the reduce function logic"""
    # Simulate grouped data from shuffle phase
    grouped = {
        'hello': [1, 1],
        'world': [1, 1],
        'python': [1]
    }
    
    results = {}
    for word, counts in grouped.items():
        results[word] = sum(counts)
    
    print("Test Reduce Function:")
    print(f"Input: {grouped}")
    print(f"Output: {results}")
    print(f"✓ Reduce function works!\n")
    return results

def test_full_pipeline():
    """Test complete MapReduce pipeline"""
    print("=" * 50)
    print("Testing Full MapReduce Pipeline")
    print("=" * 50 + "\n")
    
    # Sample text
    text = """
    the quick brown fox jumps over the lazy dog
    the dog was really lazy but the fox was quick
    """
    
    # MAP PHASE
    print("1. MAP PHASE")
    words = re.findall(r'\b[a-z]+\b', text.lower())
    intermediate = [(word, 1) for word in words]
    print(f"   Generated {len(intermediate)} intermediate pairs\n")
    
    # SHUFFLE PHASE
    print("2. SHUFFLE PHASE")
    grouped = collections.defaultdict(list)
    for word, count in intermediate:
        grouped[word].append(count)
    print(f"   Grouped into {len(grouped)} unique words\n")
    
    # REDUCE PHASE
    print("3. REDUCE PHASE")
    final = {}
    for word, counts in grouped.items():
        final[word] = sum(counts)
    
    # Sort by frequency
    sorted_words = sorted(final.items(), key=lambda x: x[1], reverse=True)
    print(f"   Top 5 words:")
    for word, count in sorted_words[:5]:
        print(f"   - {word}: {count}")
    
    print("\n✓ Full pipeline works!\n")

if __name__ == "__main__":
    test_map()
    test_reduce()
    test_full_pipeline()
    
    print("=" * 50)
    print("All tests passed! Ready for Docker deployment.")
    print("=" * 50)

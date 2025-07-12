#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import time
import random
import string
from typing import Dict, Generator, Set

import xxhash

# --- Trie Implementation ---

class TrieNode:
    """A node in the HashTrie, containing children, endpoints, and a lock."""
    def __init__(self):
        self.children: Dict[int, 'TrieNode'] = {}
        self.endpoints: Set[str] = set()
        self.lock = asyncio.Lock()

class HashTrie:
    """A trie for efficient longest prefix matching in an async environment."""

    def __init__(self, chunk_size: int = 128):
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer.")
        self.root = TrieNode()
        self.chunk_size = chunk_size

    def _chunk_and_hash(self, text: str) -> Generator[int, None, None]:
        for i in range(0, len(text), self.chunk_size):
            yield xxhash.xxh64(text[i : i + self.chunk_size].encode('utf-8')).intdigest()

    async def insert(self, request: str, endpoint: str) -> None:
        node = self.root
        async with node.lock:
            node.endpoints.add(endpoint)
        for chunk_hash in self._chunk_and_hash(request):
            async with node.lock:
                child_node = node.children.get(chunk_hash)
                if not child_node:
                    child_node = TrieNode()
                    node.children[chunk_hash] = child_node
            node = child_node
            async with node.lock:
                node.endpoints.add(endpoint)

    async def longest_prefix_match_original(self, request: str, available_endpoints: Set[str]) -> None:
        node = self.root
        match_length = 0
        chunk_hashes = self._chunk_and_hash(request)
        selected_endpoints = available_endpoints

        for i, chunk_hash in enumerate(chunk_hashes):
            async with node.lock:
                if chunk_hash in node.children:

                    node = node.children[chunk_hash]

                    # reached longest prefix match in currently-available endpoints.
                    if not node.endpoints.intersection(selected_endpoints):
                        break

                    match_length += self.chunk_size
                    selected_endpoints = node.endpoints.intersection(selected_endpoints)
                else:
                    break

    async def longest_prefix_match_new(self, request: str, available_endpoints: Set[str]) -> None:
        node = self.root
        match_length = 0
        selected_endpoints = available_endpoints

        for chunk_hash in self._chunk_and_hash(request):
            async with node.lock:
                node = node.children.get(chunk_hash)
            if not node:
                break
            async with node.lock:
                endpoints = node.endpoints.copy()
            intersection = endpoints.intersection(selected_endpoints)
            # reached longest prefix match in currently-available endpoints.
            if not intersection:
                break
            match_length += self.chunk_size
            selected_endpoints = intersection

# --- Benchmark Setup ---

async def run_workload(trie, method_name, num_tasks, available_endpoints, request_string):
    """Runs a specified number of concurrent tasks against a trie method."""
    method_to_call = getattr(trie, method_name)
    tasks = [
        asyncio.create_task(method_to_call(request_string, available_endpoints))
        for _ in range(num_tasks)
    ]
    start_time = time.perf_counter()
    await asyncio.gather(*tasks)
    end_time = time.perf_counter()
    return end_time - start_time

async def main():
    """Main function to set up and run the benchmark."""
    # --- Configuration ---
    NUM_ENDPOINTS = 500
    NUM_CONCURRENT_TASKS = 10000
    REQUEST_LENGTH = 2048
    
    print("--- Trie Performance Benchmark ---")
    print(f"Configuration: {NUM_ENDPOINTS} endpoints, {NUM_CONCURRENT_TASKS} concurrent tasks.")
    
    # --- Setup ---
    trie = HashTrie(chunk_size=128)
    endpoints = {f"endpoint_{i}" for i in range(NUM_ENDPOINTS)}
    request_string = ''.join(random.choices(string.ascii_lowercase, k=REQUEST_LENGTH))

    print("\nSetting up the trie with initial data...")
    # Insert a common prefix for all endpoints
    initial_prefix = request_string[:REQUEST_LENGTH // 2]
    for endpoint in endpoints:
        await trie.insert(initial_prefix, endpoint)
    print("Setup complete.")

    # --- Benchmarking ---
    print("\nRunning benchmark for the original method (longer lock holding)...")
    duration_original = await run_workload(
        trie, "longest_prefix_match_original", NUM_CONCURRENT_TASKS, endpoints, request_string
    )
    print(f"  -> Completed in: {duration_original:.4f} seconds")

    print("\nRunning benchmark for the new method (shorter lock holding)...")
    duration_new = await run_workload(
        trie, "longest_prefix_match_new", NUM_CONCURRENT_TASKS, endpoints, request_string
    )
    print(f"  -> Completed in: {duration_new:.4f} seconds")

    # --- Results ---
    print("\n--- Results ---")
    if duration_new < duration_original:
        improvement = (duration_original - duration_new) / duration_original * 100
        print(f"The new method was faster by {improvement:.2f}%.")
    else:
        print("The original method was unexpectedly faster.")

if __name__ == "__main__":
    asyncio.run(main())

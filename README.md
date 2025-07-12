# longest-prefix-match-benchmark

Configuration: 500 endpoints, 10000 concurrent tasks.

Setting up the trie with initial data...
Setup complete.

Running benchmark for the original method (longer lock holding)...
  -> Completed in: 1.1529 seconds

Running benchmark for the new method (shorter lock holding)...
  -> Completed in: 0.8646 seconds

--- Results ---
The new method was faster by 25.01%.

# SortedBlocks
Append-only data structure usefully for storing sorted data.


TODO:
1. Need to move this logic based on nodejs streams.
2. Need to switch to 1 byte search or start of header rather than window based as the start of header can fall on either edges of the window reads being made.
3. Should switch to binary reads instead of UTF8 to support compressed file being written.
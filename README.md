# SortedBlocks
It is a append-only data structure used in conjunction with custom blocks to persist data on underlying store.
1. Can be used to create embeded database.
2. Lock free data structure for reading and writting.
3. Supports multiple writers without sync, cause of its append nature.
4. Supports Multi version concurrency.(M.V.C.C)
5. Supports custom blocks.
6. Supports consolidation or defrag of store.
7. Upcoming support for indexing for faster reads. 
8. Out of box support for local caching.


## Binary Block Format
This is the format used to append blocks in the store, where each block is formatted as follows.
<table>
  <tr border="1px solid black" >
    <td align="center" colspan="6">
      <table>
      <tr><td align="center" colspan="2"><b>Block-Body</b></td></tr>
      <tr><td>Byte[]</td><td><4294967295Bytes</td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td align="center" colspan="6">
      <table>
      <tr><td align="center" colspan="2"><b>Block-Header</b></td></tr>
      <tr><td>Byte[]</td><td><4294967295Bytes</td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>Block-Header-Length</b></td></tr>
      <tr><td>UInt32BE</td><td>4Bytes</td></tr>
      </table>
    </td>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>Block-Body-Length</b></td></tr>
      <tr><td>UInt32BE</td><td>4Bytes</td></tr>
      </table>
    </td>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>Block-Type</b></td></tr>
      <tr><td>UInt32BE</td><td>4Bytes</td></tr>
      </table>
    </td>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>Preamble-CRC-16</b></td></tr>
      <tr><td>UInt16BE</td><td>2Bytes</td></tr>
      </table>
    </td>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>Preamble-CRC-16</b></td></tr>
      <tr><td>UInt16BE</td><td>2Bytes</td></tr>
      </table>
    </td>
    <td>
      <table>
      <tr><td align="center" colspan="2"><b>SOB</b></td></tr>
      <tr><td>0x23,0x21(#!)</td><td>2Bytes</td></tr>
      </table>
    </td>
  </tr>
</table>
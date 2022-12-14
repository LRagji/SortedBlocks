# SortedBlocks
Append-only data structure usefully for storing sorted data.


TODO:
1. Need to move this logic based on nodejs streams.
2. Need to switch to 1 byte search or start of header rather than window based as the start of header can fall on either edges of the window reads being made.
3. Should switch to binary reads instead of UTF8 to support compressed file being written.

## Append Format
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

## Format
The whole file is divided into blocks each block adheres to given below format. The concept of having multiple writters and append only is achived via blocks as each block is independent and has relative byte position internally no 2 writers can mess up the appending to the disk or cause corruption. Block are further divided into folloeing sections

1. **Preamble**: This is the start of the block and typically contains a marker called S.O.P ie: start of packet, Readers look at this specific marker to start reading.
2. **Header**: This section is hash verified and contains important key range filters and Block information that is saved with each block.
3. **Block Index**: Place where section pointers are located, It is equivalent to index.
4. **Block Data**: Place where actual key and its data is parked.
<tr><td>Block-Body</td></tr>
<table>
  <tr>
    <th>Field No</th>
    <th>Section</th>
    <th>Name</th>
    <th>Datatype</th>
    <th>Bytes</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>1</td>
    <td rowspan="4">Preamble</td>
    <td>SOP</td>
    <td>Byte[]</td>
    <td>4</td>
    <td>Magic Sequence for indicating  start of a block</td>
  </tr>
   <tr>
    <td>2</td>
    <td>HeaderHash</td>
    <td>Byte[]</td>
    <td>16</td>
    <td>MD5 Hash of the Header Block</td>
  </tr>
   <tr>
    <td>3</td>
    <td>Version</td>
    <td>UInt8BE</td>
    <td>1</td>
    <td>Version of the protocol.</td>
  </tr>
  <tr>
    <td>4</td>
    <td>HeaderHash</td>
    <td>Byte[]</td>
    <td>16</td>
    <td>MD5 Hash of the Header Block(Repeat for validity correctness against bitflips)</td>
  </tr>
  <tr>
    <td>5</td>
    <td rowspan="10">Header</td>
    <td>Block Index Length</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>Length of the root index</td>
  </tr>
  <tr>
    <td>6</td>
    <td>Block Data Length</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>Length of the root data</td>
  </tr>
  <tr>
    <td>7</td>
    <td>BlockInfo Length</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>Length of blockinfo</td>
  </tr>
  <tr>
    <td>8</td>
    <td>BlockInfo</td>
    <td>Byte[]</td>
    <td>?</td>
    <td>BlockInfo data</td>
  </tr>
  <tr>
    <td>9</td>
    <td>Block Index Hash</td>
    <td>Byte[]</td>
    <td>16</td>
    <td>MD5 of the Block Index section</td>
  </tr>
  <tr>
    <td>10</td>
    <td>Block Data Hash</td>
    <td>Byte[]</td>
    <td>16</td>
    <td>MD5 of the Root Data section</td>
  </tr>
  <tr>
    <td>11</td>
    <td>Key Bucket Factor</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>Bucketing Factor used for key binning ie: Key - (Key % BucketFactor) </td>
  </tr>
  <tr>
    <td>12</td>
    <td>Key Max</td>
    <td>UInt64BE</td>
    <td>8</td>
    <td>Maximum of the key in this block</td>
  </tr>
   <tr>
    <td>12</td>
    <td>Key Min</td>
    <td>UInt64BE</td>
    <td>8</td>
    <td>Minimum of the key in this block</td>
  </tr>
   <tr>
    <td>13</td>
    <td>EOP</td>
    <td>Byte[]</td>
    <td>4</td>
    <td>Magic Sequence for indicating end of a block</td>
  </tr>
   <tr>
    <td>14</td>
    <td rowspan="3">Block Index</td>
    <td>Section Index</td>
    <td>UInt64BE</td>
    <td>8</td>
    <td>Found by key-(key % bucket factor), it helps in bucketing the key.</td>
  </tr>
   <tr>
    <td>15</td>
    <td>Section Offset</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>A relative offset from the end of the current index, where the actuak keys for this bucket will be specified.</td>
  </tr>
  <tr bgcolor="green"  >
   <td>16</td>
   <td colspan="4">Field 14 and 15 repeats in same fashion for N sections formed by the payload keys, Total length of this index is given by field 5.</td>
  </tr>
   <tr>
    <td>17</td>
    <td rowspan="1">Block Data</td>
    <td>Sections</td>
    <td>BYTE[]</td>
    <td>Given by field 6</td>
    <td>Refer to following format for section representation bytewise.</td>
  </tr>
</table>

## Section Data

# SortedBlocks
Append-only data structure usefully for storing sorted data.


TODO:
1. Need to move this logic based on nodejs streams.
2. Need to switch to 1 byte search or start of header rather than window based as the start of header can fall on either edges of the window reads being made.
3. Should switch to binary reads instead of UTF8 to support compressed file being written.

## Format

### 1. Preamble 
This is the unsecured part of the protocol which is not hashed, and so are some fields repeated to avoid bit flips errors.
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
    <td>Root Index Length</td>
    <td>UInt32BE</td>
    <td>4</td>
    <td>Length of the root index</td>
  </tr>
  <tr>
    <td>6</td>
    <td>Root Data Length</td>
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
    <td>Root Index Hash</td>
    <td>Byte[]</td>
    <td>16</td>
    <td>MD5 of the Root Index section</td>
  </tr>
  <tr>
    <td>10</td>
    <td>Root Data Hash</td>
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
    <td>Int64BE</td>
    <td>8</td>
    <td>Maximum of the key in this block</td>
  </tr>
   <tr>
    <td>12</td>
    <td>Key Min</td>
    <td>Int64BE</td>
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
</table>
//Does GZIP have CRC32?
import { gzipSync, unzipSync } from 'node:zlib';
const originalData = Buffer.alloc(450, 0);
originalData[50] = 1;
originalData[150] = 30;
originalData[200] = 1;
const zipped = gzipSync(originalData);
//zipped[51] = 4;//Simulate Corruption
//console.log(zlib.unzipSync(zipped, { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
console.log(`Original:${originalData.length} Compressed:${zipped.length}`);

//Does GZIP allow Partial Decompression?
console.log(unzipSync(zipped).toString());
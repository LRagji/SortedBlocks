//Does GZIP have CRC32?
const zlib = require('node:zlib');
const originalData = Buffer.alloc(450, 0);
originalData[50] = 1;
originalData[150] = 30;
originalData[200] = 1;
const zipped = zlib.gzipSync(originalData);
//zipped[51] = 4;//Simulate Corruption
//console.log(zlib.unzipSync(zipped, { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
console.log(`Original:${originalData.length} Compressed:${zipped.length}`);

//Does GZIP allow Partial Decompression?
console.log(zlib.unzipSync(zipped.subarray(0, 39), { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
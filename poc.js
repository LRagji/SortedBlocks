//Does GZIP have CRC32?
const zlib = require('node:zlib');
const originalData = Buffer.from("Compression helps to reduce the data size plus has inbuilt correction codes.jbzldflkdavmd;vmsd;lmv;'l,sdv',sd',v',sdv,sd';,v'sd,vds',");
const zipped = zlib.gzipSync(originalData);
zipped[51] = 4;//Simulate Corruption
//console.log(zlib.unzipSync(zipped, { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
console.log(`Original:${originalData.length} Compressed:${zipped.length}`);

//Does GZIP allow Partial Decompression?
console.log(zlib.unzipSync(zipped.subarray(0, 39), { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
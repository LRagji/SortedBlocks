//Does GZIP have CRC32?
import { gzipSync, unzipSync } from 'node:zlib';

await new Promise((acc, rej) => {
    const zipped = [];
    for (let counter = 0; counter < 10000; counter++) {
        const payload = `${counter}-Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum`
        zipped.push(gzipSync(Buffer.from(payload, "utf-8")));
    }
    console.log("Zipping completed");

    while (zipped.length > 0) {
        const data = zipped.pop();
        const deflated = unzipSync(data).toString("utf-8");
        if (deflated.startsWith((zipped.length).toString()) === false) {
            //throw new ;
            rej(Error(`Expected: ${(zipped.length).toString()} Actual:${deflated.substring(0, 10)}`))
        }
    }
    console.log("Application completed");
    acc();
});

//zipped[51] = 4;//Simulate Corruption
//console.log(zlib.unzipSync(zipped, { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString());
// console.log(`Original:${originalData.length} Compressed:${zipped.length}`);

// //Does GZIP allow Partial Decompression?
// console.log(unzipSync(zipped).toString());
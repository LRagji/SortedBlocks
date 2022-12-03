import path from "node:path";
import * as crypto from 'node:crypto';

const testId = crypto.randomUUID();

import { Version1SortedBlocks } from '../../../source/sorted-blocks.js';
import { FileStore } from './filestore.js';

function generatePayload(time: bigint, tagId: bigint, payload: Map<bigint, Buffer>): Map<bigint, Buffer> {
    //for(let tagIdx = 0n; tagIdx <totalTags; tagIdx++) {
    const sample = Buffer.alloc(8 + 8 + 16);//Time,Value,Quality
    sample.writeBigUInt64BE(time);//Time
    sample.writeDoubleBE(12 + 0.5, 8);//Value
    sample.writeBigUInt64BE(tagId, 16);//Quality 1
    sample.writeBigUInt64BE(tagId), 24;//Quality 2
    payload.set(tagId, sample);
    //}
    return payload;
}

function purge(fileName: string, payload: Map<bigint, Buffer>) {
    const blockInfo = Buffer.from(JSON.stringify({ "dt": Date.now() }));
    const store = new FileStore(path.join("./tests/examples/timeseries/data", testId, fileName));
    try {
        Version1SortedBlocks.serialize(store, blockInfo, payload);
    }
    finally {
        store.close();
        console.log(`Purged:${testId}/${fileName} with ${payload.size} samples.`);
    }
}

function write() {
    const timePartitionAlog = (time: bigint): bigint => time - (time % BigInt(3600000)); //1 Hour
    const tagPartitionAlog = (tag: bigint): bigint => tag - (tag % BigInt(3000)); //Max IOPS
    const totalTags = BigInt(5999);
    const totalTime = BigInt(3600);
    let purgeFileName = "";

    const purgeAcc = new Map<bigint, Buffer>();
    for (let time = BigInt(0); time < totalTime; time++) {
        for (let tagIdx = BigInt(0); tagIdx < totalTags; tagIdx++) {
            const fileName = `${tagPartitionAlog(tagIdx)}-${timePartitionAlog(time)}.wal`;
            if (purgeFileName !== fileName) {
                if (purgeFileName !== "") {
                    purge(purgeFileName, purgeAcc);
                }
                purgeFileName = fileName;
                purgeAcc.clear();
            }
            generatePayload(time, tagIdx, purgeAcc);
        }
    }

    if (purgeAcc.size > 0) {
        purge(purgeFileName, purgeAcc);
    }
}

write();

// function read() {
//     const filePath = "tests/examples/timeseries/data/4c197082-043f-4ee9-a2d3-3540783ea9ba/0-0.wal";
// }
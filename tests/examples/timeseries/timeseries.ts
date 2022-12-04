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

//==================================================================================Read==========================================================================

function read() {
    const st = Date.now();
    const filePath = "tests/examples/timeseries/data/4c197082-043f-4ee9-a2d3-3540783ea9ba/0-0.wal";
    const store = new FileStore(filePath, 4096);
    //let block: null | Version1SortedBlocks = null;
    let blockCounter = 0;
    let offset = store.size();
    while (offset > 0) {
        const block = Version1SortedBlocks.deserialize(store, offset);
        if (block != null) {
            const tagId = BigInt(200);
            const info = block.meta.blockInfo.toString();
            const serializedSample = block.get(tagId);
            if (serializedSample == null) {
                throw new Error(`${info}: Key cannot be null`);
            }
            // const q2 = serializedSample.readBigUInt64BE(24);
            // const q1 = serializedSample.readBigUInt64BE(16);
            // const value = serializedSample.readDoubleBE(8);
            // const time = serializedSample.readBigUInt64BE(0);
            // if (q1 !== tagId) throw new Error(`${info}: Quality Error 1`);
            // if (q2 !== q1) throw new Error(`${info}: Quality Error 2`);
            // if (value !== (12 + 0.5)) throw new Error(`${info}: Value Error`);
            offset = block.nextBlockOffset();
            blockCounter++;
        }
    }
    const elapsed = Date.now() - st;
    const stats = store.statistics();
    const readStats = Array.from(stats.readOps.values()).reduce((acc, e) => {
        acc.max = Math.max(acc.max, e);
        acc.min = Math.min(acc.min, e);
        acc.sum = acc.sum + e;
        return acc;
    }, { min: Number.MAX_SAFE_INTEGER, max: Number.MIN_SAFE_INTEGER, sum: 0 });
    console.log(`Reading completed with ${blockCounter} blocks within ${elapsed}ms with OPS: Total:${readStats.sum} Max:${readStats.max} Min:${readStats.min} Avg:${(readStats.sum / stats.readOps.size).toFixed(2)} `);
}

read();
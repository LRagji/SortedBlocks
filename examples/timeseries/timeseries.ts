import path from "node:path";
import * as crypto from 'node:crypto';

const testId = crypto.randomUUID();

import { Version1SortedBlocks } from '../../source/sorted-blocks.js';
import { FileStore } from './filestore.js';
import { LocalCache } from "../../source/cache-proxy.js";
const dataDirectory = path.join(__dirname, "../../../data/", testId);

function generatePayload(time: bigint, tagId: bigint, payload: Map<bigint, Buffer>): Map<bigint, Buffer> {
    const sample = Buffer.alloc(8 + 8 + 16);//Time,Value,Quality
    sample.writeBigUInt64BE(time);//Time
    sample.writeDoubleBE(12 + 0.5, 8);//Value
    sample.writeBigUInt64BE(tagId, 16);//Quality 1
    sample.writeBigUInt64BE(tagId), 24;//Quality 2
    payload.set(tagId, sample);
    return payload;
}
//==================================================================================Write==========================================================================
function purge(filePath: string, payload: Map<bigint, Buffer>) {
    const blockInfo = Buffer.from(JSON.stringify({ "dt": Date.now() }));
    const store = new FileStore(filePath);
    try {
        Version1SortedBlocks.serialize(store, blockInfo, payload);
    }
    finally {
        store.close();
        //console.log(`Purged:${filePath} with ${payload.size} samples.`);
    }
}

function write() {
    const timePartitionAlgo = (time: bigint): bigint => time - (time % BigInt(3600000)); //1 Hour
    const tagPartitionAlgo = (tag: bigint): bigint => tag - (tag % BigInt(3000)); //Max IOPS
    const diskSelection = (tagPart: bigint, totalDisks: bigint): bigint => tagPart % totalDisks;
    const generatePath = (tag: bigint, time: bigint, diskPaths: string[]): string => {
        const tagPart = tagPartitionAlgo(tag), timePart = timePartitionAlgo(time);
        const diskPart = parseInt(diskSelection(tagPart, BigInt(diskPaths.length)).toString(), 10);
        return path.join(diskPaths[diskPart], `${tagPart}-${timePart}`, `raw.hb`);
    }
    const totalTags = BigInt(1000000);
    const totalTime = BigInt(3600);
    let purgeFileName = "";

    const purgeAcc = new Map<bigint, Buffer>();
    for (let time = BigInt(0); time < totalTime; time++) {
        const st = Date.now();
        const uniqueFileNames = new Set();
        for (let tagIdx = BigInt(0); tagIdx < totalTags; tagIdx++) {
            const fileName = generatePath(tagIdx, time, [dataDirectory]);//`${timePartitionAlgo(tagIdx)}-${timePartitionAlgo(time)}.wal`;
            if (purgeFileName !== fileName) {
                if (purgeFileName !== "") {
                    uniqueFileNames.add(purgeFileName);
                    purge(purgeFileName, purgeAcc);
                }
                purgeFileName = fileName;
                purgeAcc.clear();
            }
            generatePayload(time, tagIdx, purgeAcc);
        }
        console.log(`${time}: ${Date.now() - st}ms Files:${uniqueFileNames.size}`);
    }

    if (purgeAcc.size > 0) {
        purge(purgeFileName, purgeAcc);
    }
}

//==================================================================================Read==========================================================================

function read() {
    const st = Date.now();
    const filePath = path.join("/Users/105050656/Documents/Git/Personal/sorted-blocks/examples/timeseries/data/1MillTags60Seconds/0-0/raw.hb");
    const store = new FileStore(filePath, 4096);
    const cache = new LocalCache();
    let blockCounter = 0;
    let offset = store.size();
    while (offset > 0) {
        const block = Version1SortedBlocks.deserialize(store, offset, cache);
        if (block != null) {
            const tagId = BigInt(128);
            const info = block.meta.blockInfo.toString();
            const serializedSample = block.get(tagId);
            if (serializedSample == null) {
                throw new Error(`Key:${tagId} cannot be null`);
            }
            if (info == null) {
                throw new Error(`BlockInfo cannot be null`);
            }
            // const q2 = serializedSample.readBigUInt64BE(24);
            // const q1 = serializedSample.readBigUInt64BE(16);
            // const value = serializedSample.readDoubleBE(8);
            // const time = serializedSample.readBigUInt64BE(0);
            // if (q1 !== tagId) throw new Error(`${info}: Quality Error 1`);
            // if (q2 !== q1) throw new Error(`${info}: Quality Error 2`);
            // if (value !== (12 + 0.5)) throw new Error(`${info}: Value Error`);
            offset = block.meta.nextBlockOffset;
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

    //Stats
    let logLine = "";
    let totalBytes = 0;
    offset = store.size();
    cache.statistics().forEach((stats, weight) => {
        logLine += `${weight}[${(stats.Bytes / Math.max(stats.Hits, 1)).toFixed(3)}]: Bytes:${stats.Bytes} Count:${stats.Count} Hits:${stats.Hits}\n`;
        totalBytes += stats.Bytes;
    });
    console.log(`Reading completed with ${blockCounter} blocks within ${elapsed}ms with 
    OPS: Total:${readStats.sum} Max:${readStats.max} Min:${readStats.min} Avg:${(readStats.sum / stats.readOps.size).toFixed(2)}
    Cache:[${((totalBytes / offset) * 100).toFixed(2)}%] Memory:~${totalBytes}Bytes File:${offset}Bytes 
    ${logLine}`);
}

//==================================================================================Defrag==========================================================================
function defrag() {
    const st = Date.now();
    const sourcefilePath = path.join("/Users/105050656/Documents/Git/Personal/sorted-blocks/examples/timeseries/data/1MillTags60Seconds/0-0/raw.hb");
    const source = new FileStore(sourcefilePath, 4096);
    const destinationfilePath = path.join("/Users/105050656/Documents/Git/Personal/sorted-blocks/examples/timeseries/data/1MillTags60Seconds/0-0/defrag.hb");
    const destination = new FileStore(destinationfilePath, 4096);
    const sourceCache = null//new LocalCache();
    const defragOffset = source.size();
    const valueReducer = (acc: Buffer | null, e: Buffer | null, info: Buffer | null): Buffer | null => {
        if (acc != null && e != null) {
            return Buffer.concat([acc, e]);
        }
        else {
            return e;
        }
    };
    const infoReducer = (acc: Buffer, e: Buffer): Buffer => {
        const accContent = acc.toString() || JSON.stringify(["Defraged Info"]);
        const accObject = JSON.parse(accContent) as string[];
        accObject.push(e.toString());
        return Buffer.from(JSON.stringify(accObject));
    };
    Version1SortedBlocks.defrag(source, defragOffset, destination, sourceCache, undefined, valueReducer, infoReducer);
    const elapsed = Date.now() - st;
    console.log(`Defrag completed within ${elapsed}ms of ${defragOffset}Bytes`);
}

//==================================================================================Execute==========================================================================
//write();
read();
//defrag();
import path from "node:path";
import * as crypto from 'node:crypto';

const testId = crypto.randomUUID();

import { FileStore } from './filestore.js';
import { ISampleValuePart, TSBlock } from "./ts-block";
import { Blocks } from "../../source/index";
const dataDirectory = path.join(__dirname, "../../../data/");
const hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();

//==================================================================================Write==========================================================================
function write() {
    console.time("Write");
    const timePartitionAlgo = (time: bigint): bigint => time - (time % BigInt(3600000)); //1 Hour
    const tagPartitionAlgo = (tag: bigint): bigint => tag - (tag % BigInt(3000)); //Max IOPS
    const diskSelection = (tagPart: bigint, totalDisks: bigint): bigint => tagPart % totalDisks;
    const generatePath = (tag: bigint, time: bigint, diskPaths: string[]): string => {
        const tagPart = tagPartitionAlgo(tag), timePart = timePartitionAlgo(time);
        const diskPart = parseInt(diskSelection(tagPart, BigInt(diskPaths.length)).toString(), 10);
        return path.join(diskPaths[diskPart], `${tagPart}-${timePart}`, `raw.hb`);
    }
    const purge = (purgeAcc: Map<bigint, Map<bigint, ISampleValuePart>>, purgeFileName: string, partitions: Map<string, Blocks>) => {
        const block = TSBlock.parse(purgeAcc);
        let blocks = partitions.get(purgeFileName)
        if (blocks == null) {
            blocks = new Blocks(new FileStore(purgeFileName));
            partitions.set(purgeFileName, blocks);
        }
        const bytes = blocks.append(block);
        //console.log(`Purged ${bytes}bytes for ${purgeFileName}`);
    };

    const totalTags = BigInt(1000000);
    const totalTime = BigInt(60);//In seconds

    let purgeFileName = "";
    const purgeAcc = new Map<bigint, Map<bigint, ISampleValuePart>>();
    const partitions = new Map<string, Blocks>();
    for (let time = BigInt(0); time < totalTime; time++) {
        for (let tagIdx = BigInt(0); tagIdx < totalTags; tagIdx++) {
            const timeValues = purgeAcc.get(tagIdx) || new Map<bigint, ISampleValuePart>();
            timeValues.set(time * BigInt(1000), { value: parseFloat(`${Number(tagIdx)}.${Number(time)}`), quality: hashResolver(Buffer.from(`${Number(tagIdx)}.${Number(time)}`)) });
            purgeAcc.set(tagIdx, timeValues);
            const fileName = generatePath(tagIdx, time, [path.join(dataDirectory, testId)]);//`${timePartitionAlgo(tagIdx)}-${timePartitionAlgo(time)}.wal`;
            if (purgeFileName !== fileName) {
                if (purgeFileName !== "") {
                    purge(purgeAcc, purgeFileName, partitions);
                }
                purgeFileName = fileName;
                purgeAcc.clear();
            }
        }
    }

    if (purgeAcc.size > 0) {
        purge(purgeAcc, purgeFileName, partitions);
    }

    partitions.forEach((block, filePath) => {
        (block.store as FileStore).close();
        console.log(`Closed:${filePath}`);
    });

    console.timeEnd("Write");
}

//==================================================================================Read==========================================================================

function read() {
    const st = Date.now();
    const filePath = path.join(dataDirectory, "DataSet/3000-0/raw.hb");
    const store = new FileStore(filePath, 4096);
    const blockSet = new Blocks(store);
    const cursor = blockSet.iterate(new Map([[TSBlock.blockType, TSBlock.from]]));
    let blockCounter = 0;
    let result = cursor.next();
    while (!result.done) {
        const block = result.value[0] as TSBlock;
        const tags = Array.from(block.tags());
        const samples = block.samples(BigInt(12), () => true);
        console.log(`#${block.blockPosition}: Tags:${tags.length}`);
        blockCounter++;
        result = cursor.next();
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
    console.log(`Reading completed with ${blockCounter} blocks within ${elapsed}ms with 
    OPS: Total:${readStats.sum} Max:${readStats.max} Min:${readStats.min} Avg:${(readStats.sum / stats.readOps.size).toFixed(2)}`);
}

//==================================================================================Defrag==========================================================================
// function defrag() {
//     const st = Date.now();
//     const sourcefilePath = path.join(dataDirectory, "1MillTags60Seconds/0-0/raw.hb");
//     const source = new FileStore(sourcefilePath, 4096);
//     const destinationfilePath = path.join(dataDirectory, "1MillTags60Seconds/0-0/defrag.hb")
//     const destination = new FileStore(destinationfilePath, 4096);
//     const sourceCache = null//new LocalCache();
//     const defragOffset = source.size();
//     const valueReducer = (acc: Buffer | null, e: Buffer | null, info: Buffer | null): Buffer | null => {
//         if (acc != null && e != null) {
//             return Buffer.concat([acc, e]);
//         }
//         else {
//             return e;
//         }
//     };
//     const infoReducer = (acc: Buffer, e: Buffer): Buffer => {
//         const accContent = acc.toString() || JSON.stringify(["Defraged Info"]);
//         const accObject = JSON.parse(accContent) as string[];
//         accObject.push(e.toString());
//         return Buffer.from(JSON.stringify(accObject));
//     };
//     Version1SortedBlocks.defrag(source, defragOffset, destination, sourceCache, undefined, valueReducer, infoReducer);
//     const elapsed = Date.now() - st;
//     console.log(`Defrag completed within ${elapsed}ms of ${defragOffset}Bytes`);
// }

//==================================================================================Execute==========================================================================
// write();
read();
//defrag();
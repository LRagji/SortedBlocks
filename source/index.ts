interface ILevelStats { "min": bigint, "max": bigint, keys: bigint[], buckets: Set<bigint>, factor: bigint }
/**
 * This function take a hierarchichal parent child values and converts them into a disk based hashset.
 * It expects values only on leaf node, all other nodes are represented by Uint64 number.
 * @param payload A map where Key is a Uint64 array which has parent child nodes or path(Parent -> Child;Root -> Leaf) to the value, Value is any Arraybuffer, 
 * @returns A buffer which can be written on disk. Data Structre is documented here:{@link https://github.com/LRagji/sorted-blocks/blob/Version2/documentation/Protocol.xlsx}
 */
export function append(payload: Map<BigUint64Array, ArrayBuffer>): { "serialized": ArrayBuffer, "metaBytes": number, "contentBytes": number } {//4KB since thats is the minimum one IO operation on disk through which we will read the payload.
    const leafNodeIndexOverheadBytes = 8;
    const leafNodeIndexConsumptionPerEntryBytes = 16;
    const parentNodeIndexOverheadBytes = 28;
    const parentNodeIndexConsumptionPerEntryBytes = 12;
    const headerBytes = 16;
    let returnBuffer = Buffer.alloc(0);
    let contentBufferSizeBytes = 0;
    let metaBufferSizeBytes = 0;
    let hierarchyDepth = 0;

    if (payload.size === 0) {
        throw new Error(`Parameter "payload" cannot be empty.`);
    }

    const levelsMinandMax = new Map<number, ILevelStats>();
    payload.forEach((value: ArrayBuffer, hierarchicalPCR: BigUint64Array) => {//) Index is a parent to Nth index is child
        if (hierarchicalPCR.length < 1) {
            throw new Error(`Parameter "payload" cannot have a hierarchy depth less than 1.`);
        } else {
            if (hierarchyDepth === 0) {
                hierarchyDepth = hierarchicalPCR.length;
            }
            else if (hierarchicalPCR.length != hierarchyDepth) {
                throw new Error(`Parameter "payload" cannot have inconsistent hierarchy depth, expected depth ${hierarchyDepth}.`);
            }
        }
        contentBufferSizeBytes += value.byteLength;

        hierarchicalPCR.forEach((key, level) => {
            const existingStats = levelsMinandMax.get(level) || { "min": key, "max": key, keys: [], buckets: new Set<bigint>(), factor: BigInt(0) };
            if (existingStats.min > key) existingStats.min = key;
            if (existingStats.max < key) existingStats.max = key;
            existingStats.keys.push(key);
            levelsMinandMax.set(level, existingStats);
        });
    });

    levelsMinandMax.forEach((stats, level) => {
        stats = findBestBucketFactor(stats);
        if (level === (levelsMinandMax.size - 1)) {//Since the last level has less overheads
            metaBufferSizeBytes += leafNodeIndexOverheadBytes + (leafNodeIndexConsumptionPerEntryBytes * stats.buckets.size);
        }
        else {
            metaBufferSizeBytes += parentNodeIndexOverheadBytes + (parentNodeIndexConsumptionPerEntryBytes * stats.buckets.size);
        }
    });
    metaBufferSizeBytes += headerBytes;

    return { "contentBytes": contentBufferSizeBytes, "metaBytes": metaBufferSizeBytes, "serialized": returnBuffer };
}

function findBestBucketFactor(stats: ILevelStats): ILevelStats {
    stats.factor = BigInt(16);//Think of some nice algo to come up with factor using min max
    stats.buckets = stats.keys.reduce((acc, key) => { acc.add(key - (key % stats.factor)); return acc }, stats.buckets);
    return stats;
}
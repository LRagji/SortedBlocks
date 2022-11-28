import crypto from 'node:crypto';
import { IAppendStore } from './i-append-store';
import { SortedSection } from './sorted-section';

export class Version1SortedBlocks {

    //TODO:
    // 1. Make Keys UINT61 from Int64(helps in modding)
    // 1.2 Change Modding algo to start of bucket
    // 2. Change Terminology according to google sheet document.
    // 3. Move googlee sheet to Readme as md
    // 3.5 Should we Keep IStore or ICursor impplemeentation?
    // 4. Think of a way to make this into KD Tree Multidimensional
    // 5. Think of Caching Tree in Redis
    // 6. Build a state machine to Read byte by byte and merge into API 
    // 7. Remove maxValueSizeInBytes from serialize
    private static readonly hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
    private static readonly magicBuffer = Version1SortedBlocks.hashResolver(Buffer.from(`16111987`));
    private static readonly SOP = Version1SortedBlocks.magicBuffer.subarray(0, 4);
    private static readonly EOP = Version1SortedBlocks.magicBuffer.subarray(12, 16);
    private static readonly version = Uint8Array.from([1]);

    public static serialize(appenOnlyStore: IAppendStore, blockInfo: Buffer, payload: Map<bigint, Buffer>, maxValueSizeInBytes = 1024): number {

        //console.time("  Sort");
        const sortedKeys = new BigInt64Array(payload.keys()).sort();
        //console.timeEnd("  Sort");

        //console.time("  Extra");
        const minKey = sortedKeys[0], maxKey = sortedKeys[sortedKeys.length - 1];
        const inclusiveKeyRange = (maxKey - minKey) + BigInt(1);
        const bucketFactor = 1024;//TODO: We will calculate this later with some algo for a given range.
        const bucketFactorBigInt = BigInt(bucketFactor);
        const keysPerSection = parseInt((inclusiveKeyRange / bucketFactorBigInt).toString(), 10) + 1;
        //console.timeEnd("  Extra");

        //Sections
        //console.time("  Sections");
        const sections = new Map<bigint, SortedSection>();
        for (let index = 0; index < sortedKeys.length; index++) {
            const key = sortedKeys[index];
            const sectionKey = key % bucketFactorBigInt;
            const section = sections.get(sectionKey) || new SortedSection(keysPerSection, maxValueSizeInBytes);
            //@ts-ignore
            section.add(key, payload.get(key)); //TODO:HotSpot in terms of performance.
            sections.set(sectionKey, section);
        }
        //console.timeEnd("  Sections");

        //Final Index
        //console.time("  Index");
        const indexDataLength = Buffer.alloc(8);
        const finalIndex = new SortedSection(sections.size, (maxValueSizeInBytes + SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes));
        sections.forEach((section, sectionKey) => {
            const iResult = section.toBuffer();
            indexDataLength.writeUInt32BE(iResult.index.length, 0);
            indexDataLength.writeUInt32BE(iResult.values.length, 4);
            const sectionBuff = Buffer.concat([indexDataLength, iResult.index, iResult.values]);
            finalIndex.add(sectionKey, sectionBuff);
        });
        sections.clear();
        const block = finalIndex.toBuffer();
        const valuesBuff = block.values;
        const indexBuff = block.index;
        //console.timeEnd("  Index");

        //Header
        //console.time("  Header");
        const dataHash = Version1SortedBlocks.hashResolver(valuesBuff);
        const IndexHash = Version1SortedBlocks.hashResolver(indexBuff);
        const iheader = new Array<number>();//TODO:Write a new expanding Array class
        const Buffer64 = Buffer.alloc(8);
        const Buffer32 = Buffer.alloc(4);
        iheader.push(...Version1SortedBlocks.EOP);
        Buffer64.writeBigInt64BE(minKey, 0);
        iheader.push(...Buffer64);
        Buffer64.writeBigInt64BE(maxKey, 0);
        iheader.push(...Buffer64);
        Buffer32.writeUInt32BE(bucketFactor, 0);
        iheader.push(...Buffer32);
        iheader.push(...dataHash);
        iheader.push(...IndexHash);
        iheader.push(...blockInfo);
        Buffer32.writeUInt32BE(blockInfo.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(valuesBuff.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(indexBuff.length, 0);
        iheader.push(...Buffer32);
        //console.timeEnd("  Header");

        //Compose Packet
        //console.time("  Packet");
        const header = Buffer.from(iheader);
        const headerHash = Version1SortedBlocks.hashResolver(header);
        //Buffer32.writeUInt32BE(header.length, 0);
        //const headerLength = Buffer.from(Buffer32);
        const dataToAppend = Buffer.concat([valuesBuff, indexBuff, header, headerHash, Version1SortedBlocks.version, headerHash, Version1SortedBlocks.SOP]);
        //console.timeEnd(" Packet");

        //Append
        //console.time("  Append");
        appenOnlyStore.append(dataToAppend);
        //console.timeEnd("  Append");
        return dataToAppend.length;
    }

    public static deserialize(appenOnlyStore: IAppendStore, offset: number): Version1SortedBlocks | null {
        const accumulatorMaxLength = Math.max(Version1SortedBlocks.SOP.length, Version1SortedBlocks.EOP.length) * 2;
        let accumulator = Buffer.alloc(0);
        const gaurdsIndexesToFind = [Version1SortedBlocks.SOP, Version1SortedBlocks.EOP];
        const gaurdsIndexes = [-1, -1];
        let state = 0;
        let returnValue: Version1SortedBlocks | null = null;
        if (offset < 0) {
            throw new Error(`Param "offset" cannot be lesser than 0.`);
        }
        let data: Buffer | null = null;
        do {
            data = appenOnlyStore.reverseRead(offset);
            if (data != null && data.length > 0) {
                accumulator = Buffer.concat([data, accumulator]);
                const indexFirstByteSOP = data.indexOf(gaurdsIndexesToFind[state][0]);//When written EOP -- -- --> SOP Left to Right When read back it will be Right to Left
                if (indexFirstByteSOP !== -1 && accumulator.length >= (gaurdsIndexesToFind[state].length + indexFirstByteSOP)
                    && gaurdsIndexesToFind[state].reduce((a, e, idx) => a && e === accumulator[indexFirstByteSOP + idx], true)) {
                    if (state === 0) {
                        gaurdsIndexes[state] = indexFirstByteSOP + gaurdsIndexesToFind[state].length;
                        //Next 2 lines done to search EOP in the same segment read, where SOP was found. Forcing the same offset be read twice
                        accumulator = Buffer.from(accumulator, data.length);
                        data = Buffer.alloc(0);
                    }
                    gaurdsIndexes[state] = indexFirstByteSOP;
                    state++;
                }
                else if (state === 0 && accumulator.length > accumulatorMaxLength) {
                    accumulator = Buffer.from(accumulator, 0, accumulatorMaxLength);
                }
                offset -= data.length;
            }
            else {
                offset = -1;
            }
        }
        while (offset > 0 && state < gaurdsIndexesToFind.length)

        if (gaurdsIndexes[0] !== -1 && gaurdsIndexes[1] !== -1) {
            const actualPosition = offset + gaurdsIndexes[1];
            accumulator = Buffer.from(accumulator, gaurdsIndexes[1], (gaurdsIndexes[0] + gaurdsIndexes[1] + 1));
            //SOP Validation
            let endIndex = accumulator.length;
            let beginIndex = endIndex - gaurdsIndexesToFind[0].length;
            if (accumulator.subarray(beginIndex, endIndex).reduce((a, e, idx) => a && e === gaurdsIndexesToFind[0][beginIndex + idx], true) === false) {
                return returnValue;
            }
            //Header Hash 1
            endIndex = beginIndex;
            beginIndex -= 16;
            const headerHash1 = accumulator.subarray(beginIndex, endIndex);
            //Version
            endIndex = beginIndex;
            beginIndex -= 1;
            const version = accumulator.subarray(beginIndex, endIndex);
            if (version.reduce((a, e, idx) => a && e === Version1SortedBlocks.version[idx], true) === false) {
                return returnValue;
            }
            //Header Hash 2
            endIndex = beginIndex;
            beginIndex -= 16;
            const headerHash2 = accumulator.subarray(beginIndex, endIndex);
            if (headerHash2.reduce((a, e, idx) => a && e === headerHash1[idx], true) === false) {
                return returnValue;
            }
            //Header Hash Match
            const computedHash = Version1SortedBlocks.hashResolver(accumulator.subarray(0, beginIndex));
            if (headerHash2.reduce((a, e, idx) => a && e === computedHash[idx], true) === false) {
                return returnValue;
            }
            //RootIndexLength
            endIndex = beginIndex;
            beginIndex -= 4;
            const rootIndexLength = accumulator.subarray(beginIndex, endIndex).readUint32BE();
            //RootDataLength
            endIndex = beginIndex;
            beginIndex -= 4;
            const rootDataLength = accumulator.subarray(beginIndex, endIndex).readUint32BE();
            //BlockInfoLength
            endIndex = beginIndex;
            beginIndex -= 4;
            const blockInfoLength = accumulator.subarray(beginIndex, endIndex).readUint32BE();
            //BlockInfo
            endIndex = beginIndex;
            beginIndex -= blockInfoLength;
            const blockInfo = accumulator.subarray(beginIndex, endIndex);
            //RootIndexHash
            endIndex = beginIndex;
            beginIndex -= 16;
            const rootIndexHash = accumulator.subarray(beginIndex, endIndex);
            //RootDataHash
            endIndex = beginIndex;
            beginIndex -= 16;
            const rootDataHash = accumulator.subarray(beginIndex, endIndex);
            //RootBucketFactor
            endIndex = beginIndex;
            beginIndex -= 4;
            const rootBucketFactor = accumulator.subarray(beginIndex, endIndex).readUint32BE();
            //RootMax
            endIndex = beginIndex;
            beginIndex -= 8;
            const rootMax = accumulator.subarray(beginIndex, endIndex).readBigInt64BE();
            //RootMin
            endIndex = beginIndex;
            beginIndex -= 8;
            const rootMin = accumulator.subarray(beginIndex, endIndex).readBigInt64BE();
            //EOP
            endIndex = beginIndex;
            beginIndex -= gaurdsIndexesToFind[1].length;
            if (accumulator.subarray(beginIndex, endIndex).reduce((a, e, idx) => a && e === gaurdsIndexesToFind[1][beginIndex + idx], true) === false) {
                return returnValue;
            }

            return new Version1SortedBlocks(actualPosition, rootMin, rootMax, rootBucketFactor, blockInfo, rootIndexHash, rootDataHash, rootIndexLength, rootDataLength, headerHash1, actualPosition + accumulator.length, appenOnlyStore);
        }

        return returnValue;
    }

    constructor(
        private readonly actualHeaderEndPoisition: number,
        public readonly KeyRangeMin: bigint,
        public readonly KeyRangeMax: bigint,
        private readonly keyBucketFactor: number,
        public readonly BlockInfo: Buffer,
        private readonly indexHash: Buffer,
        private readonly dataHash: Buffer,
        private readonly indexLength: number,
        private readonly dataLength: number,
        private readonly headerHash: Buffer,
        private readonly actualHeaderStart: number,
        private readonly appenOnlyStore: IAppendStore,
    ) { }

    public get(key: Buffer): Buffer {
        throw new Error("TBI");
    }

    public *iterate(): Generator<[key: Buffer, value: Buffer]> {
        throw new Error("TBI");
    }

    public cache(includeSubSections = false): Buffer {
        throw new Error("TBI");
    }
}

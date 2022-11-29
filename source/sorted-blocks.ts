import crypto from 'node:crypto';
import { IAppendStore } from './i-append-store';
import { SortedSection } from './sorted-section';

export class Version1SortedBlocks {

    //TODO:
    // 0. Change algorith to start of group with mod
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

    public static serialize(appendOnlyStore: IAppendStore, blockInfo: Buffer, payload: Map<bigint, Buffer>, maxValueSizeInBytes = 1024): number {

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
        appendOnlyStore.append(dataToAppend);
        //console.timeEnd("  Append");
        return dataToAppend.length;
    }

    public static deserialize(appendOnlyStore: IAppendStore, offset: number): Version1SortedBlocks | null {
        const accumulatorMaxLength = Math.max(Version1SortedBlocks.SOP.length, Version1SortedBlocks.EOP.length) * 2;
        let accumulator = Buffer.alloc(0);
        const gaurdsIndexesToFind = [Version1SortedBlocks.SOP, Version1SortedBlocks.EOP];
        let state = 0;
        let returnValue: Version1SortedBlocks | null = null;
        let data: Buffer | null = null;
        let actualStartPosition = -1, actualEndPosition = -1;
        do {
            data = appendOnlyStore.reverseRead(offset);
            if (data != null && data.length > 0) {
                accumulator = Buffer.concat([data, accumulator]);
                let indexOfFirstByte = -1;
                do {
                    indexOfFirstByte = data.indexOf(gaurdsIndexesToFind[state][0], (indexOfFirstByte + 1));//When written EOP -- -- --> SOP Left to Right When read back it will be Right to Left
                    if (indexOfFirstByte !== -1 && accumulator.length >= (gaurdsIndexesToFind[state].length + indexOfFirstByte)
                        && gaurdsIndexesToFind[state].reduce((a, e, idx) => a && e === accumulator[indexOfFirstByte + idx], true)) {
                        if (state === 0) {
                            actualStartPosition = (offset - data.length) + (indexOfFirstByte + 1 + gaurdsIndexesToFind[state].length);
                            accumulator = accumulator.subarray(0, (indexOfFirstByte + gaurdsIndexesToFind[state].length));//Trims the right end of the buffer
                            //Next 2 lines written to search EOP in the same segment read, where SOP was found. Forcing the same offset be read twice
                            accumulator = accumulator.subarray(data.length);//Trim the recently added data only
                            data = Buffer.alloc(0);
                        }
                        else if (state === 1) {
                            accumulator = accumulator.subarray(indexOfFirstByte);
                            actualEndPosition = offset - (data.length - indexOfFirstByte);
                        }
                        indexOfFirstByte = -1;
                        state++;
                    }
                    else if (state === 0 && accumulator.length > data.length + accumulatorMaxLength) {
                        //Trim the segment so that it doesnt grow infinitely when SOP is not found.
                        accumulator = accumulator.subarray(0, data.length + accumulatorMaxLength);
                    }
                }
                while (indexOfFirstByte !== -1)
                offset -= data.length;
            }
            else {
                offset = -1;
            }
        }
        while (offset > 0 && state < gaurdsIndexesToFind.length)

        if (state === gaurdsIndexesToFind.length) {
            //SOP Validation
            let endIndex = accumulator.length;
            let beginIndex = endIndex - gaurdsIndexesToFind[0].length;
            if (accumulator.subarray(beginIndex, endIndex).reduce((a, e, idx) => a && e === gaurdsIndexesToFind[0][idx], true) === false) {
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
            if (accumulator.subarray(beginIndex, endIndex).reduce((a, e, idx) => a && e === gaurdsIndexesToFind[1][idx], true) === false) {
                return returnValue;
            }

            return new Version1SortedBlocks({
                "actualHeaderStartPosition": actualStartPosition,
                "actualHeaderEndPosition": actualEndPosition,
                "keyRangeMin": rootMin,
                "keyRangeMax": rootMax,
                "keyBucketFactor": BigInt(rootBucketFactor),
                "blockInfo": blockInfo,
                "indexHash": rootIndexHash,
                "dataHash": rootDataHash,
                "indexLength": rootIndexLength,
                "dataLength": rootDataLength,
                "headerHash": headerHash1,
                "storeId": appendOnlyStore.Id
            }, appendOnlyStore);
        }

        return returnValue;
    }

    private readonly sectionToAbsoluteOffsetPointers = new Map<bigint, number>();
    private readonly keysToValueOffset = new Map<number, Map<bigint, { absStart: number, absEnd: number }>>();
    constructor(
        public readonly meta: {
            actualHeaderEndPosition: number,
            keyRangeMin: bigint,
            keyRangeMax: bigint,
            keyBucketFactor: bigint,
            blockInfo: Buffer,
            indexHash: Buffer,
            dataHash: Buffer,
            indexLength: number,
            dataLength: number,
            headerHash: Buffer,
            actualHeaderStartPosition: number,
            storeId: string
        },
        private readonly appendOnlyStore: IAppendStore
    ) {
        if (this.meta == null) {
            throw new Error(`Parameter "meta" is needed for the block to be constructed.`)
        }
        if (this.appendOnlyStore == null) {
            throw new Error(`Parameter "appendOnlyStore" is needed for the block to be constructed.`)
        }
        if (this.meta.storeId != this.appendOnlyStore.Id) {
            throw new Error(`Store ids should match [${this.meta.storeId} = ${this.appendOnlyStore.Id}].`)
        }
    }

    public get(key: bigint): Buffer | null {
        if (key > this.meta.keyRangeMax || key < this.meta.keyRangeMin) {
            return null;
        }
        const keySection = key - key % this.meta.keyBucketFactor;
        if (this.sectionToAbsoluteOffsetPointers.size > 0 && !this.sectionToAbsoluteOffsetPointers.has(keySection)) {
            return null;
        }
        if (this.sectionToAbsoluteOffsetPointers.size === 0) {
            //Fill the map
            let readOffset = this.meta.actualHeaderEndPosition;
            let accumulator = Buffer.alloc(0);
            let data: Buffer | null = Buffer.alloc(0);
            while (readOffset < (this.meta.actualHeaderEndPosition + this.meta.indexLength) && data != null) {
                accumulator = Buffer.concat([data, accumulator]);
                readOffset += data.length;
                data = this.appendOnlyStore.reverseRead(readOffset);
            }
            accumulator = accumulator.subarray(accumulator.length - this.meta.indexLength);
            const computedHash = Version1SortedBlocks.hashResolver(accumulator);
            if (this.meta.indexHash.reduce((a, e, idx) => a && e === computedHash[idx], true) === false) {
                throw new Error(`Data Integrity check failed! Index hash do not match actual:${this.meta.indexHash} expected:${computedHash}}.`);
            }
            readOffset = accumulator.length;
            let start = readOffset, end = readOffset;
            while (readOffset > 0) {
                end = start;
                start -= 8;
                const sectionKey = accumulator.subarray(start, end).readBigInt64BE();
                end = start;
                start -= 4;
                const relativeOffset = accumulator.subarray(start, end).readUint32BE();
                const absoluteOffset = this.meta.actualHeaderEndPosition + this.meta.indexLength + relativeOffset;
                this.sectionToAbsoluteOffsetPointers.set(sectionKey, absoluteOffset);
                readOffset = start;
            }
        }
        const absoluteOffset = this.sectionToAbsoluteOffsetPointers.get(keySection);
        if (absoluteOffset == undefined) {
            return null;
        }
        //This means we have the section key
        let kvPointer = this.keysToValueOffset.get(absoluteOffset);
        if (kvPointer == undefined) {
            //Read from the disk
            const bytesForIndexLength = 4, bytesForDataLength = 4;
            let readOffset = absoluteOffset;
            let accumulator = Buffer.alloc(0);
            let data: Buffer | null = Buffer.alloc(0);
            while (readOffset < (absoluteOffset + bytesForIndexLength + bytesForDataLength) && data != null) {
                accumulator = Buffer.concat([data, accumulator]);
                readOffset -= data.length;
                data = this.appendOnlyStore.reverseRead(readOffset);
            }
            accumulator = accumulator.subarray(accumulator.length - (bytesForDataLength + bytesForIndexLength));
            const indexLength = accumulator.subarray(0, bytesForIndexLength).readUint32BE();
            const dataLength = accumulator.subarray(bytesForIndexLength, (bytesForDataLength + bytesForIndexLength)).readUint32BE();

            readOffset = absoluteOffset - bytesForIndexLength - bytesForDataLength;
            data = Buffer.alloc(0);
            accumulator = Buffer.alloc(0);
            while (readOffset <= (absoluteOffset - bytesForIndexLength - bytesForDataLength - indexLength) && data != null) {
                accumulator = Buffer.concat([data, accumulator]);
                readOffset -= data.length;
                data = this.appendOnlyStore.reverseRead(readOffset);
            }
            accumulator = accumulator.subarray(accumulator.length - (bytesForDataLength + bytesForIndexLength + indexLength));
            readOffset = accumulator.length;
            let start = readOffset, end = readOffset;
            let actualKeyOffsetMap = this.keysToValueOffset.get(absoluteOffset) || new Map<bigint, { absStart: number, absEnd: number }>();
            while (readOffset > 0) {
                end = start;
                start -= 8;
                const actualKey = accumulator.subarray(start, end).readBigInt64BE();
                end = start;
                start -= 4;
                const relativeOffset = accumulator.subarray(start, end).readUint32BE();
                actualKeyOffsetMap.set(actualKey, { absStart: (absoluteOffset - (bytesForIndexLength + bytesForDataLength + indexLength + relativeOffset)), absEnd: 0 });
                readOffset = start;
            }
            const values = Array.from(actualKeyOffsetMap.values());
            for (let index = (values.length - 1); index > 0; index--) {
                if (index === values.length - 1) {
                    values[index].absEnd = absoluteOffset - (bytesForIndexLength + bytesForDataLength + indexLength + dataLength);

                }
                values[index - 1].absEnd = values[index].absStart;
            }
            this.keysToValueOffset.set(absoluteOffset, actualKeyOffsetMap);
            kvPointer = this.keysToValueOffset.get(absoluteOffset);
        }
        if (kvPointer == undefined) {
            throw new Error(`Data Integrity check failed!, section ${absoluteOffset} returnign empty list, run full block data integrity check.`)
        }
        const absoluteSpace = kvPointer.get(key);
        if (absoluteSpace == undefined) {
            return null;
        }
        let readOffset = absoluteSpace.absStart;
        let data: Buffer | null = Buffer.alloc(0);
        let accumulator = Buffer.alloc(0);
        while (readOffset <= absoluteSpace.absEnd && data != null) {
            accumulator = Buffer.concat([data, accumulator]);
            readOffset -= data.length;
            data = this.appendOnlyStore.reverseRead(readOffset);
        }
        accumulator = accumulator.subarray(accumulator.length - (absoluteSpace.absStart = absoluteSpace.absEnd));
        return accumulator;
    }

    public * iterate(): Generator<[key: bigint, value: Buffer]> {
        throw new Error("TBI");
    }
}

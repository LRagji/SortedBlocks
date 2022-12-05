import crypto from 'node:crypto';
import { IAppendStore } from './i-append-store';
import { SortedSection } from './sorted-section';

export class Version1SortedBlocks {

    //TODO:
    // 1 Next block offset is missing.
    // 2. Change Terminology according to google sheet document.
    // 3. Move googlee sheet to Readme as md
    // 3.5 Should we Keep IStore or ICursor impplemeentation?
    // 4. Think of a way to make this into KD Tree Multidimensional
    // 5. Think of Caching Tree in Redis
    // 6. Build a state machine to Read byte by byte and merge into API 
    // 7. Remove maxValueSizeInBytes from serialize
    // 8. What can be done to not keep on allocating Buffers for a million time in serialize command. 74GB
    // 9. Add validation for data hash via flag.
    //10. reduce readiops from 9k in get.
    //11. Add option to validate entire data.
    private static readonly hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
    private static readonly magicBuffer = Version1SortedBlocks.hashResolver(Buffer.from(`16111987`));
    private static readonly SOP = Version1SortedBlocks.magicBuffer.subarray(0, 4);
    private static readonly EOP = Version1SortedBlocks.magicBuffer.subarray(12, 16);
    private static readonly version = Uint8Array.from([1]);
    private static readonly maxBigInt = BigInt("18446744073709551615");
    private static readonly minBigInt = BigInt("0");


    public static serialize(appendOnlyStore: IAppendStore, blockInfo: Buffer, payload: Map<bigint, Buffer>, maxValueSizeInBytes = 1024): number {

        //console.time("  Sort");
        const sortedKeys = new BigInt64Array(payload.keys()).sort();
        //console.timeEnd("  Sort");

        //console.time("  Extra");
        const minKey = sortedKeys[0], maxKey = sortedKeys[sortedKeys.length - 1];
        if (minKey < Version1SortedBlocks.minBigInt) throw new Error(`Only positive keys of 64bit values are allowed,from ${Version1SortedBlocks.minBigInt}) to ${Version1SortedBlocks.maxBigInt} [${minKey}]`);
        if (maxKey > Version1SortedBlocks.maxBigInt) throw new Error(`Only positive keys of 64bit values are allowed,from ${Version1SortedBlocks.minBigInt}) to ${Version1SortedBlocks.maxBigInt} [${maxKey}]`);
        const bucketFactor = 1024;//TODO: We will calculate this later with some algo for a given range.
        const bucketFactorBigInt = BigInt(bucketFactor);
        //console.timeEnd("  Extra");

        //Sections
        //console.time("  Sections");
        let maxIndexBytes = 0, maxPointerBytes = 0;
        const sections = new Map<bigint, SortedSection>();
        for (let index = 0; index < sortedKeys.length; index++) {
            const key = sortedKeys[index];
            const sectionKey = key - key % bucketFactorBigInt;
            const section = sections.get(sectionKey) || new SortedSection(bucketFactor, maxValueSizeInBytes);
            //@ts-ignore
            const bytesWritten = section.add(key, payload.get(key)); //TODO:HotSpot in terms of performance.
            maxIndexBytes = Math.max(maxIndexBytes, bytesWritten.indexLength);
            maxPointerBytes = Math.max(maxPointerBytes, bytesWritten.payloadLength);
            sections.set(sectionKey, section);
        }
        //console.timeEnd("  Sections");

        //Final Index
        //console.time("  Index");
        const indexDataLength = Buffer.alloc(8);
        const finalIndex = new SortedSection(sections.size, (maxIndexBytes + maxPointerBytes + indexDataLength.length));
        sections.forEach((section, sectionKey) => {
            const iResult = section.toBuffer();
            indexDataLength.writeUint32BE(iResult.index.length, 0);
            indexDataLength.writeUint32BE(iResult.values.length, 4);
            const sectionBuff = Buffer.concat([iResult.values, iResult.index, indexDataLength]);
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
        Buffer64.writeBigUInt64BE(BigInt.asUintN(64, minKey), 0);
        iheader.push(...Buffer64);
        Buffer64.writeBigUInt64BE(BigInt.asUintN(64, maxKey), 0);
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
            const rootMax = accumulator.subarray(beginIndex, endIndex).readBigUint64BE();
            //RootMin
            endIndex = beginIndex;
            beginIndex -= 8;
            const rootMin = accumulator.subarray(beginIndex, endIndex).readBigUint64BE();
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
                "storeId": appendOnlyStore.Id,
                "nextBlockOffset": actualEndPosition - (rootIndexLength + rootDataLength)
            }, appendOnlyStore);
        }

        return returnValue;
    }

    public static defrag(source: IAppendStore, sourceOffset: number, destination: IAppendStore,
        purgeCallback = (mergedBlocks: Map<bigint, Buffer>) => false,
        valueReduce = (reducedValue: Buffer | null, value: Buffer | null, blockInfo: Buffer | null): Buffer | null => null,
        blockInfoReduce = (mergedBlockinfo: Buffer, newblockInfo: Buffer) => Buffer.alloc(0)) {
        const blocks = new Array<Version1SortedBlocks>();
        let mergedBlockInfo = Buffer.alloc(0);
        while (sourceOffset > 0) {
            const block = Version1SortedBlocks.deserialize(source, sourceOffset);
            if (block != null) {
                blocks.push(block);
                sourceOffset = block.meta.nextBlockOffset;
                mergedBlockInfo = blockInfoReduce(mergedBlockInfo, block.meta.blockInfo);
            }
        }

        const processedKeys = new Set<bigint>();
        const mergedBlock = new Map<bigint, Buffer>();
        while (blocks.length > 0) {
            const block = blocks.pop();
            if (block != undefined) {
                const iterator = block.iterate();
                let kvp = iterator.next();
                while (!kvp.done) {
                    const key = kvp.value[0];
                    const value = kvp.value[1];
                    let mergedValue = blocks
                        .map(b => b.get(key))
                        .reduce((acc, e) => valueReduce(acc, e, block.meta.blockInfo), value);
                    if (mergedBlock.has(key) === true) {
                        mergedValue = valueReduce(mergedValue, mergedBlock.get(key) || null, null);
                    }
                    if (mergedValue != null) {
                        mergedBlock.set(key, mergedValue);
                    }
                    processedKeys.add(key);
                    if (purgeCallback(mergedBlock) === true) {
                        Version1SortedBlocks.serialize(destination, mergedBlockInfo, mergedBlock, Array.from(mergedBlock.values()).reduce((acc, e) => Math.max(acc, e.length), 0));
                        mergedBlock.clear();
                    }
                    kvp = iterator.next();
                }
            }
        }
        if (mergedBlock.size > 0) {
            Version1SortedBlocks.serialize(destination, mergedBlockInfo, mergedBlock, Array.from(mergedBlock.values()).reduce((acc, e) => Math.max(acc, e.length), 0));
            mergedBlock.clear();
        }
    }

    public IndexIntegrityPassed: boolean = false;
    private memoryCache = new Map<string, Buffer>();

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
            storeId: string,
            nextBlockOffset: number
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
        if (key < Version1SortedBlocks.minBigInt) throw new Error(`Only positive keys of 64bit values are allowed,from ${Version1SortedBlocks.minBigInt}) to ${Version1SortedBlocks.maxBigInt} [${key}]`);
        if (key > Version1SortedBlocks.maxBigInt) throw new Error(`Only positive keys of 64bit values are allowed,from ${Version1SortedBlocks.minBigInt}) to ${Version1SortedBlocks.maxBigInt} [${key}]`);
        if (key > this.meta.keyRangeMax || key < this.meta.keyRangeMin) {
            return null;
        }
        const keySection = key - key % this.meta.keyBucketFactor;
        let sectionOffset = this.getSectionOffset(keySection);

        if (sectionOffset != null) {
            const valuePointer = this.getValuePointer(sectionOffset, key);
            if (valuePointer != null) {
                return this.efficientReversedRead(valuePointer.absStart, valuePointer.absEnd);
            }
        }
        return null;
    }

    public * iterate(): Generator<[key: bigint, value: Buffer]> {
        throw new Error("TBI");
    }

    private efficientReversedRead(fromPosition: number, tillPosition: number, cache: boolean = false): Buffer {
        const cacheKey = `${fromPosition}:${tillPosition}`;
        let returnValue: Buffer | undefined = this.memoryCache.get(cacheKey);
        if (returnValue != undefined) {
            return returnValue;
        }
        let accumulator = Buffer.alloc(0);
        let data: Buffer | null = Buffer.alloc(0);
        let startPosition = fromPosition;
        while (startPosition > tillPosition && data != null) {
            accumulator = Buffer.concat([data, accumulator]);
            startPosition -= data.length;
            data = this.appendOnlyStore.reverseRead(startPosition);
        }
        returnValue = accumulator.subarray(accumulator.length - (fromPosition - tillPosition));
        if (cache === true) {
            this.memoryCache.set(cacheKey, Buffer.from(returnValue));
        }
        return returnValue
    }

    private getSectionOffset(sectionStart: bigint): number | undefined {
        let sectionOffset: number | undefined = undefined;
        let accumulator = this.efficientReversedRead(this.meta.actualHeaderEndPosition, this.meta.actualHeaderEndPosition - this.meta.indexLength, true);
        if (this.IndexIntegrityPassed === false) {
            const computedHash = Version1SortedBlocks.hashResolver(accumulator);
            this.IndexIntegrityPassed = this.meta.indexHash.reduce((a, e, idx) => a && e === computedHash[idx], true);
            if (this.IndexIntegrityPassed === false) {
                throw new Error(`Data Integrity check failed! Index hash do not match actual:${this.meta.indexHash} expected:${computedHash}}.`);
            }
        }
        let readOffset = accumulator.length;
        let start = readOffset, end = readOffset;
        while (readOffset > 0) {
            end = start;
            start -= 8;
            const sectionKey = accumulator.subarray(start, end).readBigUint64BE();
            end = start;
            start -= 4;
            const relativeOffset = accumulator.subarray(start, end).readUint32BE();
            const absoluteOffset = this.meta.actualHeaderEndPosition - (this.meta.indexLength + relativeOffset);
            if (sectionKey === sectionStart) {
                sectionOffset = absoluteOffset;
                break;
            }
            readOffset = start;
        }
        return sectionOffset;
    }

    private getValuePointer(sectionOffset: number, key: bigint): { absStart: number, absEnd: number } | null {
        const bytesForIndexLength = 4, bytesForDataLength = 4;
        let accumulator = this.efficientReversedRead(sectionOffset, (sectionOffset - (bytesForIndexLength + bytesForDataLength)), true);
        const indexLength = accumulator.subarray(0, bytesForIndexLength).readUint32BE();
        const dataLength = accumulator.subarray(bytesForIndexLength, (bytesForDataLength + bytesForIndexLength)).readUint32BE();
        accumulator = this.efficientReversedRead(sectionOffset - (bytesForIndexLength + bytesForDataLength), (sectionOffset - (bytesForIndexLength + bytesForDataLength + indexLength)), true);
        let readOffset = accumulator.length;
        let start = readOffset, end = readOffset;
        let valuePointer: { absStart: number, absEnd: number } | null = null;
        while (readOffset > 0) {
            end = start;
            start -= 8;
            const actualKey = accumulator.subarray(start, end).readBigUint64BE();
            end = start;
            start -= 4;
            const relativeOffset = accumulator.subarray(start, end).readUint32BE();
            if (valuePointer == null) {
                if (actualKey === key) {
                    valuePointer = { absStart: (sectionOffset - (bytesForIndexLength + bytesForDataLength + indexLength + relativeOffset)), absEnd: -1 };
                }
            }
            else if (valuePointer.absEnd === -1) {
                valuePointer.absEnd = (sectionOffset - (bytesForIndexLength + bytesForDataLength + indexLength + relativeOffset));
                break;
            }
            readOffset = start;
        }
        if (valuePointer !== null && valuePointer.absEnd === -1) {
            valuePointer.absEnd = sectionOffset - (bytesForIndexLength + bytesForDataLength + indexLength + dataLength);
        }
        return valuePointer;
    }
}

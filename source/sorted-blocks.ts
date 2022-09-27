import crypto from 'node:crypto';
import { IAppendStore } from './i-append-store';
import { SortedSection } from './sorted-section';

export class Version1SortedBlocks {
    private readonly SOP = Buffer.from(`5092242716ebf7369dad22382acbbc10`);
    private readonly EOP = Buffer.from(`8c734039166ca17b2f9ef603a1aedd66`);
    private readonly version = Buffer.alloc(4)
    private readonly hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();

    constructor(private readonly appenOnlyStore: IAppendStore) { this.version.writeUInt32BE(1); }

    public put(blockId: Buffer, keys: BigInt64Array, values: Buffer[], maxValueSizeInBytes = 1024): number {
        if (keys.length != values.length) throw new Error(`Number of keys(${keys.length}) doesnot match with number of values provided(${values.length}).`);
        const sortedKeys = keys.sort();
        const minKey = sortedKeys[0], maxKey = sortedKeys[sortedKeys.length - 1];
        const inclusiveKeyRange = (maxKey - minKey) + BigInt(1);
        const bucketFactor = 1024;//TODO: We will calculate this later with some algo for a given range.
        const bucketFactorBigInt = BigInt(bucketFactor);
        const keysPerSection = parseInt((inclusiveKeyRange / bucketFactorBigInt).toString(), 10) + 1;

        //Sections
        const sections = new Map<bigint, SortedSection>();
        for (let index = 0; index < sortedKeys.length; index++) {
            const key = sortedKeys[index];
            const keyIndex = keys.indexOf(key);
            const value = values[keyIndex];
            const sectionKey = key % bucketFactorBigInt;
            const section = sections.get(sectionKey) || new SortedSection(keysPerSection, maxValueSizeInBytes);
            section.add(key, value);
            sections.set(sectionKey, section);
        }

        //Final Index
        const finalIndex = new SortedSection(sections.size, (maxValueSizeInBytes + SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes));
        sections.forEach((section, sectionKey) => {
            const iResult = section.toBuffer();
            const BufferIndexLength32 = Buffer.alloc(4);
            BufferIndexLength32.writeUInt32BE(iResult.index.length, 0);
            const BufferDataLength32 = Buffer.alloc(4);
            BufferDataLength32.writeUInt32BE(iResult.values.length, 0);
            const sectionBuff = Buffer.concat([BufferIndexLength32, BufferDataLength32, iResult.index, iResult.values]);
            finalIndex.add(sectionKey, sectionBuff);
        });
        sections.clear();
        const block = finalIndex.toBuffer();
        const valuesBuff = block.values;
        const indexBuff = block.index;

        //Header
        const dataHash = this.hashResolver(valuesBuff);
        const IndexHash = this.hashResolver(indexBuff);
        const iheader = new Array<number>();//TODO:Write a new expanding Array class
        const Buffer64 = Buffer.alloc(8);
        const Buffer32 = Buffer.alloc(4);
        iheader.push(...this.EOP);
        Buffer64.writeBigInt64BE(minKey, 0);
        iheader.push(...Buffer64);
        Buffer64.writeBigInt64BE(maxKey, 0);
        iheader.push(...Buffer64);
        Buffer32.writeUInt32BE(bucketFactor, 0);
        iheader.push(...Buffer32);
        iheader.push(...dataHash);
        Buffer32.writeUInt32BE(dataHash.length, 0);
        iheader.push(...Buffer32);
        iheader.push(...IndexHash);
        Buffer32.writeUInt32BE(IndexHash.length, 0);
        iheader.push(...Buffer32);
        iheader.push(...blockId);
        Buffer32.writeUInt32BE(blockId.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(indexBuff.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(valuesBuff.length, 0);
        iheader.push(...Buffer32);

        //Compose Packet
        const header = Buffer.from(iheader);
        const headerHash = this.hashResolver(header);
        Buffer32.writeUInt32BE(headerHash.length, 0);
        const headerHashLength = Buffer.from(Buffer32);
        Buffer32.writeUInt32BE(header.length, 0);
        const headerLength = Buffer.from(Buffer32);
        const dataToAppend = Buffer.concat([valuesBuff, indexBuff, header, headerLength, headerHash, headerHashLength, headerHash, headerHashLength, this.version, this.SOP]);

        //Write
        this.appenOnlyStore.append(dataToAppend);
        return dataToAppend.length;
    }

    public get(key: Buffer): Buffer {
        throw new Error("TBI");
    }

    public *iterate(): Generator<[key: Buffer, value: Buffer]> {
        throw new Error("TBI");
    }
}
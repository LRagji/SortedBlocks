import crypto from 'node:crypto';
import { IAppendStore } from './i-append-store';
import { SortedSection } from './sorted-section';

export class Version1SortedBlocks {
    private readonly hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
    private readonly SOP = this.hashResolver(Buffer.from(`16111987`));
    private readonly EOP = this.hashResolver(Buffer.from(`01011991`));
    private readonly version = Buffer.alloc(4)

    constructor(private readonly appenOnlyStore: IAppendStore) { this.version.writeUInt32BE(1); }

    public put(blockId: Buffer, payload: Map<bigint, Buffer>, maxValueSizeInBytes = 1024): number {

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
        //console.timeEnd("  Index");

        //Header
        //console.time("  Header");
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
        iheader.push(...IndexHash);
        iheader.push(...blockId);
        Buffer32.writeUInt32BE(blockId.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(valuesBuff.length, 0);
        iheader.push(...Buffer32);
        Buffer32.writeUInt32BE(indexBuff.length, 0);
        iheader.push(...Buffer32);
        //console.timeEnd("  Header");

        //Compose Packet
        //console.time("  Packet");
        const header = Buffer.from(iheader);
        const headerHash = this.hashResolver(header);
        Buffer32.writeUInt32BE(header.length, 0);
        const headerLength = Buffer.from(Buffer32);
        const dataToAppend = Buffer.concat([valuesBuff, indexBuff, header, headerHash, headerLength, headerHash, headerLength, this.version, this.SOP]);
        //console.timeEnd(" Packet");

        //Append
        //console.time("  Append");
        this.appenOnlyStore.append(dataToAppend);
        //console.timeEnd("  Append");
        return dataToAppend.length;
    }

    public get(key: Buffer): Buffer {
        throw new Error("TBI");
    }

    public *iterate(): Generator<[key: Buffer, value: Buffer]> {
        throw new Error("TBI");
    }
}
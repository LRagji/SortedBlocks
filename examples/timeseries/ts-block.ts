import { buffer } from 'stream/consumers';
import { Block, IAppendStore } from '../../source/index';

export interface ISampleValuePart {
    value: number;
    quality: Buffer;
}

export interface ISampleSet extends Map<bigint, Map<bigint, ISampleValuePart>> { }
export class TSBlock extends Block {
    public static version = 1;
    public static blockType: number = 100;
    public override type: number = TSBlock.blockType;

    private static versionSize = 1;
    private static tagIDSize = 8;
    private static tagSamplesLengthSize = 4;
    private static timeSize = 8;
    private static valueSize = 8;
    private static qualitySize = 8;

    private readonly tagsOffset = new Map<bigint, { start: number, end: number }>;

    constructor(private readonly headerBuff: Buffer, private readonly bodyBuff: Buffer) {
        super();
        if (headerBuff[0] != TSBlock.version) throw new Error(`Invalid version actual:${headerBuff[0]} expected:${TSBlock.version}`);
        let headerPointer = 1;
        let relativeBodyPosition = 0;
        while (headerPointer < headerBuff.length - 1) {
            const tagId = headerBuff.readBigUInt64BE(headerPointer);
            headerPointer += TSBlock.tagIDSize;
            const numberOfSamples = headerBuff.readUint32BE(headerPointer);
            headerPointer += TSBlock.tagSamplesLengthSize;
            const bytes = numberOfSamples * (TSBlock.timeSize + TSBlock.valueSize + TSBlock.qualitySize);
            this.tagsOffset.set(tagId, { start: relativeBodyPosition, end: relativeBodyPosition + bytes });
            relativeBodyPosition += bytes;
        }
    }

    public static parse(sampleSet: ISampleSet): TSBlock {
        //validate: sampleSet is not of size 0
        //validate no tags have empty time values and null in value or quality
        //validate quality is not more than 128bits (16Bytes).

        //We can zip body

        const header = Buffer.alloc(TSBlock.versionSize + (sampleSet.size * (TSBlock.tagIDSize + TSBlock.tagSamplesLengthSize)));//Version(1B)TagId1(8)Len1(4)....TagIdN(8)LenN(4)
        let headerPointer = header.writeUInt8(TSBlock.version, 0);
        let bodyBuffer = Buffer.alloc(0);
        const sortedTags = BigInt64Array.from(Array.from(sampleSet.keys())).sort();
        sortedTags.forEach(tagId => {
            const timeValues = sampleSet.get(tagId);
            if (timeValues != null) {
                const tagBodyBuffer = Buffer.alloc((TSBlock.timeSize + TSBlock.valueSize + TSBlock.qualitySize) * timeValues.size);//Time(8),Value(8),Quality(16)
                let timePointer = 0, valuePointer = timeValues.size * (TSBlock.timeSize), qualityPointer = timeValues.size * (TSBlock.timeSize + TSBlock.valueSize);
                const sortedtime = BigInt64Array.from(Array.from(timeValues.keys())).sort();
                sortedtime.forEach(time => {
                    const samplePart = timeValues.get(time);
                    const value = samplePart != null ? samplePart.value : 0.0;
                    const quality = samplePart != null ? samplePart.quality : Buffer.alloc(TSBlock.qualitySize, 0);
                    timePointer = tagBodyBuffer.writeBigUInt64BE(time, timePointer);
                    valuePointer = tagBodyBuffer.writeDoubleBE(value, valuePointer);
                    qualityPointer += quality.copy(tagBodyBuffer, qualityPointer);
                });
                headerPointer = header.writeBigInt64BE(tagId, headerPointer);
                headerPointer = header.writeInt32BE(timeValues.size, headerPointer);
                bodyBuffer = Buffer.concat([bodyBuffer, tagBodyBuffer]);
            }
        });
        return new TSBlock(header, bodyBuffer);
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): TSBlock {
        const block = super.from(store, type, blockPosition, headerLength, bodyLength);
        const header = block.header();
        if (type != TSBlock.blockType) throw new Error(`Invalid block type actual:${type} expected:${TSBlock.blockType}`);
        const returnObject = new TSBlock(header, block.body());
        returnObject.blockPosition = block.blockPosition;
        returnObject.store = block.store;
        returnObject.bodyLength = block.bodyLength;
        returnObject.headerLength = block.headerLength;
        return returnObject;
    }

    public override header(): Buffer {
        //Version(1B)TagId1(8)Len1(4)....TagIdN(8)LenN(4)
        return this.headerBuff;
    }

    public override body(): Buffer {
        //Time(8),Value(8),Quality(16)
        return this.bodyBuff;
    }

    public override merge(other: TSBlock): Block {
        if (other.type === this.type) {
            const thisSampleSet = Array.from(this.tags()).reduce((acc, tagId) => this.samples(tagId, () => true, acc), new Map<bigint, Map<bigint, ISampleValuePart>>());
            const mergedSampleSet = Array.from(other.tags()).reduce((acc, tagId) => other.samples(tagId, () => true, acc), thisSampleSet);
            return TSBlock.parse(mergedSampleSet);
        }
        else {
            throw new Error(`Cannot merge different types of block ${this.type} & ${other.type}`);
        }
    }

    public tags(): IterableIterator<bigint> {
        return this.tagsOffset.keys();
    }

    public samples(tag: bigint, filter: (time: bigint, value: number, quality: Buffer) => boolean, returnObject = new Map<bigint, Map<bigint, ISampleValuePart>>()): ISampleSet {
        const position = this.tagsOffset.get(tag);
        if (position == null) return returnObject;
        const body = this.body().subarray(position.start, position.end);
        let readPointer = 0;
        const tagSection = returnObject.get(tag) || new Map<bigint, ISampleValuePart>();
        while (readPointer < body.length - 1) {
            const time = body.readBigUInt64BE(readPointer);
            readPointer += TSBlock.timeSize;
            const floatValue = body.readDoubleBE(readPointer);
            readPointer += TSBlock.valueSize;
            const qualityBuff = Buffer.from(body.subarray(readPointer, readPointer + TSBlock.qualitySize));
            readPointer += TSBlock.qualitySize;
            if (filter(time, floatValue, qualityBuff)) {
                tagSection.set(time, { value: floatValue, quality: qualityBuff });
            }
        }
        returnObject.set(tag, tagSection);
        return returnObject;
    }


}
import { IAppendStore } from "./i-append-store";
import crc16 from 'crc/calculators/crc16';

export enum CachePolicy {
    Default, //Should only cache indexes
    None, //Clear cache after every call
}

enum SystemBlockTypes {
    Consolidated = 10,
}
export class Block {
    public type: number = 0;
    public blockPosition: number = -1;
    public headerLength: number = -1;
    public bodyLength: number = -1;
    public store: IAppendStore | null = null;

    public static from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): Block {
        if (store == null) throw new Error(`Parameter "store" cannot be null or undefined.`);
        if (type == null || type < 0 || type > MaxUint32) throw new Error(`Parameter "type" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        if (blockPosition == null || blockPosition < 0) throw new Error(`Parameter "blockPosition" cannot be null or undefined and has to be greater than 0.`);
        if (headerLength == null || headerLength < 0 || headerLength > MaxUint32) throw new Error(`Parameter "headerLength" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        if (bodyLength == null || bodyLength < 0 || bodyLength > MaxUint32) throw new Error(`Parameter "bodyLength" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        const returnObject = new Block();
        returnObject.store = store;
        returnObject.type = type;
        returnObject.blockPosition = blockPosition;
        returnObject.headerLength = headerLength;
        returnObject.bodyLength = bodyLength;
        return returnObject;
    }

    public header(): Buffer {
        return this.store?.measuredReverseRead(this.blockPosition, this.blockPosition - this.headerLength) || Buffer.alloc(0);
    }
    public body(): Buffer {
        return this.store?.measuredReverseRead((this.blockPosition - this.headerLength), this.blockPosition - (this.headerLength + this.bodyLength)) || Buffer.alloc(0);
    }
    public merge(other: Block): Block {
        throw new Error("Method not implemented.");
    }

}
export class SkipBlock extends Block {

    private readonly _header: Buffer;

    constructor(public readonly inclusivePositionFromSkip: bigint, public readonly inclusivePositionToSkip: bigint) {
        super();
        this._header = Buffer.alloc(16);
        this._header.writeBigUint64BE(this.inclusivePositionFromSkip, 0);
        this._header.writeBigUint64BE(this.inclusivePositionToSkip, 8);
        this.store = null;
        this.blockPosition = this._header.length;
        this.bodyLength = 0;
        this.headerLength = this._header.length;
        this.type = SystemBlockTypes.Consolidated;
    }

    public override header(): Buffer {
        return this._header;
    }

    public override body(): Buffer {
        return Buffer.alloc(0);
    }

    public override merge(other: Block): Block {
        throw new Error(`System Block(${this.type}):${this.store?.id} cannot be merged with another Block(${other.type}):${other.store?.id}`);
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): SkipBlock {
        const block = super.from(store, type, blockPosition, headerLength, bodyLength);
        const inclusivePositionFromSkip = block.header().readBigUint64BE(0);
        const inclusivePositionToSkip = block.header().readBigUint64BE(8);
        const returnObject = new SkipBlock(inclusivePositionFromSkip, inclusivePositionToSkip);
        returnObject.blockPosition = blockPosition;
        returnObject.store = store;
        if (returnObject.bodyLength !== bodyLength) throw new Error(`Invalid body length ${bodyLength}, must be 0.`);
        return returnObject;
    }

}

export interface IBlocksCache {
    set(absolutePosition: number, block: Block): void;
    get(absolutePosition: number): Block | undefined;
    clear(before: number | undefined, after: number | undefined): void;
    length: number;
}

export class LocalCache implements IBlocksCache {
    public readonly cache = new Map<number, Block>();

    public get length() {
        return this.cache.size;
    }

    public set(absolutePosition: number, block: Block): void {
        this.cache.set(absolutePosition, block);
    }
    public get(absolutePosition: number): Block | undefined {
        return this.cache.get(absolutePosition);
    }
    public clear(before: number | undefined = undefined, after: number | undefined = undefined): void {
        this.cache.clear();
    }

}

// export interface KVP extends IBlock {
//     version: number,
//     minKey: number,
//     maxKey: number,
//     integrityVerified: boolean,
//     index: Map<bigint, [startIndex: number, endIndex: number]>,
//     get(key: bigint): Buffer
//     iterate(ascending: boolean): IterableIterator<[key: bigint, value: Buffer]>,
//     fromMap(kvps: Map<bigint, Buffer>): KVP
// }

export const MaxUint32 = 4294967295;


export class Blocks {

    public readonly cacheContainer: IBlocksCache;
    private readonly skipPositions = new Array<{ fromPositionInclusive: number, toPositionInclusive: number }>();//Should always be sorted in desc order of position

    private storeReaderPosition: number = -1;
    private storeStartPosition: number = 0;
    private readonly store: IAppendStore;
    private readonly cachePolicy: CachePolicy;
    private readonly systemBlocks = 100;
    private readonly preambleLength = 18;
    public static readonly SOB = Buffer.from("2321", "hex");//#! 35 33
    private static readonly SystemBlockFactory = new Map<number, typeof Block.from>([[SystemBlockTypes.Consolidated, SkipBlock.from]])

    constructor(store: IAppendStore, cachePolicy: CachePolicy = CachePolicy.Default, cacheContainer: IBlocksCache = new LocalCache()) {
        this.store = store;
        this.cachePolicy = cachePolicy;
        this.cacheContainer = cacheContainer;
    }

    public append(block: Block): number {
        if (block.type > MaxUint32 || block.type < this.systemBlocks) throw new Error(`Block type must be between ${this.systemBlocks} and ${MaxUint32}.`);
        return this.systemBlockAppend(block);
    }

    public * iterate(blockTypeFactory: Map<number, typeof Block.from> | undefined = undefined): Generator<[Block, number]> {
        this.storeReaderPosition = this.positionSkipper(this.store.length - 1);
        let accumulator = Buffer.alloc(0);
        const SOBLastByte = Blocks.SOB[Blocks.SOB.length - 1];
        while (this.storeReaderPosition > this.storeStartPosition) {
            let reverserBuffer = this.store.reverseRead(this.storeReaderPosition);
            if (reverserBuffer == null || reverserBuffer.length === 0) {
                return;
            }
            accumulator = Buffer.concat([reverserBuffer, accumulator.subarray(0, Blocks.SOB.length + 1)]);
            let matchingIndex = accumulator.length;
            do {
                matchingIndex = accumulator.lastIndexOf(SOBLastByte, (matchingIndex - 1))
                if (matchingIndex !== -1
                    && (matchingIndex - (Blocks.SOB.length - 1)) >= 0
                    && Blocks.SOB.reduce((a, e, idx, arr) => a && e === accumulator[matchingIndex - ((arr.length - 1) - idx)], true)) {
                    const absoluteMatchingIndex = (this.storeReaderPosition - (reverserBuffer.length - 1)) + matchingIndex;
                    let block = this.cacheContainer.get(absoluteMatchingIndex);
                    let isBlockFromCache = true;
                    if (block == null) {
                        //construct & invoke 
                        const preamble = this.store.measuredReverseRead(absoluteMatchingIndex, Math.max(absoluteMatchingIndex - this.preambleLength, this.storeStartPosition));
                        if (preamble == null || preamble.length !== this.preambleLength) {
                            return;
                        }
                        const blockHeaderLength = preamble.readInt32BE(0);
                        const blockBodyLength = preamble.readInt32BE(4);
                        const blockType = preamble.readInt32BE(8);
                        const crc1 = preamble.readUInt16BE(12);
                        const crc2 = preamble.readUInt16BE(14);
                        if (crc1 != crc2 && crc2 != crc16(preamble.subarray(0, 12))) {
                            continue;
                        }
                        block = Block.from(this.store, blockType, absoluteMatchingIndex - this.preambleLength, blockHeaderLength, blockBodyLength);
                        isBlockFromCache = false;
                    }
                    //Construct //Reason it need to be out is cause of casting blocks with newer types even when they are in cache.
                    const constructFunction = blockTypeFactory?.get(block.type) || Blocks.SystemBlockFactory.get(block.type) || Block.from;
                    block = constructFunction(block.store as IAppendStore, block.type, block.blockPosition, block.headerLength, block.bodyLength);
                    if (this.cachePolicy != CachePolicy.None || isBlockFromCache === true) {
                        this.cacheContainer.set(absoluteMatchingIndex, block);
                    }
                    matchingIndex = -1;
                    reverserBuffer = Buffer.alloc(0);
                    accumulator = Buffer.alloc(0);
                    this.storeReaderPosition = (absoluteMatchingIndex - (this.preambleLength + block.headerLength + block.bodyLength));
                    //validate if its system block
                    if (block.type < this.systemBlocks) {
                        this.handleSystemBlock(block);
                    }
                    else {
                        yield ([block, Math.max(this.storeReaderPosition - this.storeStartPosition, this.storeStartPosition)]);
                    }
                }
            }
            while (matchingIndex > 0)
            this.storeReaderPosition -= reverserBuffer.length;
            this.storeReaderPosition = this.positionSkipper(this.storeReaderPosition);
        }
    }

    public consolidate(shouldPurge: (combinedBlock: Block) => boolean = (acc) => false, blockTypeFactory: Map<number, typeof Block.from> | undefined = undefined): boolean {
        let accumulator: Block | null = null;
        let lastToPurgePosition: number = this.storeReaderPosition, lastFromPurgePosition: number = this.storeReaderPosition;
        let currentBlock: Block | null = null;
        let onlySingleBlock = true;
        const cursor = this.iterate(blockTypeFactory);
        let result = cursor.next();
        while (!result.done) {
            currentBlock = result.value[0];
            if (accumulator == null) {
                accumulator = currentBlock;
                lastFromPurgePosition = currentBlock.blockPosition + this.preambleLength;
            }
            else {
                onlySingleBlock = false;
                accumulator = currentBlock.merge(accumulator);
                lastToPurgePosition = currentBlock.blockPosition - (currentBlock.headerLength + currentBlock.bodyLength);
                if (shouldPurge(accumulator) === true) {
                    this.purgeConsolidatedBlocks(accumulator, lastFromPurgePosition, lastToPurgePosition);
                }
            }
            result = cursor.next();
        }
        if (accumulator != null && onlySingleBlock === false) {
            this.purgeConsolidatedBlocks(accumulator, lastFromPurgePosition, lastToPurgePosition);
        }

        return !onlySingleBlock;
    }

    public index() {
        //This indexing the blocks in the file and appends a index block to the store, difference is it does not move or duplicate data like consolidate function.
        throw new Error("TBI");
    }

    private handleSystemBlock(systemBlock: Block): void {
        switch (systemBlock.type) {
            case SystemBlockTypes.Consolidated:
                const castedBlock: SkipBlock = systemBlock as SkipBlock;
                this.skipPositions.push({ fromPositionInclusive: Number(castedBlock.inclusivePositionFromSkip), toPositionInclusive: Number(castedBlock.inclusivePositionToSkip) });
                //Sort the skip positions
                this.skipPositions.sort((a, b) => b.toPositionInclusive - b.toPositionInclusive);
                break;

            default:
                break;
        }
    }

    private positionSkipper(position: number): number {
        return this.skipPositions.reduce((acc, s) => {
            if (acc >= s.toPositionInclusive && acc <= s.fromPositionInclusive) {
                acc = s.toPositionInclusive - 1;
            }
            return acc;
        }, position);
    }

    private systemBlockAppend(block: Block): number {
        const blockBody = block.body();
        const blockHeader = block.header();
        if (blockBody.length > MaxUint32) throw new Error(`Block body size cannot be more than ${MaxUint32}.`);
        if (blockHeader.length > MaxUint32) throw new Error(`Block header size cannot be more than ${MaxUint32}.`);

        const preamble = Buffer.alloc(18);
        preamble.writeUInt32BE(blockHeader.length);
        preamble.writeUInt32BE(blockBody.length, 4);
        preamble.writeUInt32BE(block.type, 8);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 12);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 14);
        preamble.writeUint8(Blocks.SOB[0], 16);
        preamble.writeUint8(Blocks.SOB[1], 17);
        const finalBuffer = Buffer.concat([blockBody, blockHeader, preamble]);

        this.store.append(finalBuffer);
        return finalBuffer.length;
    }

    private purgeConsolidatedBlocks(accumulator: Block | null, lastFromPurgePosition: number, lastToPurgePosition: number) {
        const inclusivePositionToSkip = BigInt(lastToPurgePosition + 1);
        const inclusivePositionFromSkip = BigInt(lastFromPurgePosition);
        const skip = new SkipBlock(inclusivePositionFromSkip, inclusivePositionToSkip);
        this.append(accumulator as Block);
        this.systemBlockAppend(skip);
        accumulator = null;
        this.cacheContainer.clear(undefined, undefined);
    }
}

import { IAppendStore } from "./i-append-store";
import crc16 from 'crc/calculators/crc16';
import { SystemBlockTypes } from "./system-block-types";
import { IBlocksCache } from "./i-blocks-cache";
import { LocalCache } from "./local-cache";
import { SkipBlock } from "./skip-block";
import { Block } from "./block";
import { CachePolicy } from "./cache-policy";

export const MaxUint32 = 4294967295;
export class Blocks {

    public readonly cacheContainer: IBlocksCache;
    public readonly store: IAppendStore;

    private readonly skipPositions = new Array<{ fromPositionInclusive: number, toPositionInclusive: number }>();//Should always be sorted in desc order of position

    private storeReaderPosition: number = -1;
    private storeStartPosition: number = 0;
    private readonly cachePolicy: CachePolicy;
    private readonly systemBlocks = 100;
    private readonly preambleLength = 18;
    public static readonly SOB = Buffer.from("2321", "hex");//#! 35 33

    /***
     * Creates a new instance of Blocks.
     */
    constructor(store: IAppendStore, cachePolicy: CachePolicy = CachePolicy.Default, cacheContainer: IBlocksCache = new LocalCache()) {
        this.store = store;
        this.cachePolicy = cachePolicy;
        this.cacheContainer = cacheContainer;
    }

    public append(block: Block): Promise<number> {
        if (block.type > MaxUint32 || block.type < this.systemBlocks) throw new Error(`Block type must be between ${this.systemBlocks} and ${MaxUint32}.`);
        return this.systemBlockAppend(block);
    }

    public * iterate(blockTypeFactory: (block: Block) => Block = (b) => b): Generator<[Block, number]> {
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
                    let isBlockFromCache = true;
                    let block = this.cacheContainer.get(absoluteMatchingIndex);
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

                        //Construct
                        block = Block.from(this.store, blockType, absoluteMatchingIndex - this.preambleLength, blockHeaderLength, blockBodyLength);
                        if (block.type >= this.systemBlocks) block = blockTypeFactory(block);
                        if (this.cachePolicy != CachePolicy.None) {
                            this.cacheContainer.set(absoluteMatchingIndex, block);
                        }
                        isBlockFromCache = false;
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
                        if (isBlockFromCache === true) block = blockTypeFactory(block);//This is the case when block was cached it was casted into some different type now its of different type.
                        yield ([block, Math.max(this.storeReaderPosition - this.storeStartPosition, this.storeStartPosition)]);
                    }
                }
            }
            while (matchingIndex > 0)
            this.storeReaderPosition -= reverserBuffer.length;
            this.storeReaderPosition = this.positionSkipper(this.storeReaderPosition);
        }
    }

    public consolidate(shouldPurge: (combinedBlock: Block) => boolean = (acc) => false, blockTypeFactory: (block: Block) => Block = (b) => b): boolean {
        //TODO: We need to implement skipping mechanishm, as we may find blocks of different types which cannot be merged.
        //TODO: We need to include position of next block in purge callback so that the user urderstands where they are in the process.
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
        //TODO: Skip blocks can have multiple entries.
        switch (systemBlock.type) {
            case SystemBlockTypes.Consolidated:
                const castedBlock: SkipBlock = SkipBlock.from(systemBlock.store as IAppendStore, systemBlock.type, systemBlock.blockPosition, systemBlock.headerLength, systemBlock.bodyLength);
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

    private async systemBlockAppend(block: Block): Promise<number> {
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

        await this.store.append(finalBuffer);
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


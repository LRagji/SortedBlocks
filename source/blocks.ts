import { IAppendStore } from "./i-append-store";
import crypto from 'node:crypto';

export enum CacheStrategy {
    Default, //Should only cache indexes
    None, //Clear cache after every call
    All//Should also cache values
}

export interface IBlock {
    type: number;
    header: Buffer,
    bodyStartIndex: number,
    startIndex: number,
    endIndex: number,
    storeId: string,
    merge(another: IBlock): IBlock,
    toBuffer(): Buffer
    fromStore(store: IAppendStore): IBlock
}

export interface KVP extends IBlock {
    version: number,
    minKey: number,
    maxKey: number,
    integrityVerified: boolean,
    index: Map<bigint, [startIndex: number, endIndex: number]>,
    get(key: bigint): Buffer
    iterate(ascending: boolean): IterableIterator<[key: bigint, value: Buffer]>,
    fromMap(kvps: Map<bigint, Buffer>): KVP
}

export interface Defrag extends IBlock {
    DefraggedStart: number
}
export const MaxUint32 = 4294967295;
export const HashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
export const SOB = HashResolver(Buffer.from("MAGICSEQUENCE"));

export class Blocks {

    public readonly cachedBlocks = new Array<IBlock>();

    private sourceParsedIndex: number = -1;
    private sourceIndexToStopAt: number = 0;
    private readonly source: IAppendStore;
    private readonly cacheStrategy: CacheStrategy;

    public append(block: IBlock): number {
        const serialized = block.toBuffer();
        if (serialized.length > MaxUint32) throw new Error(`Block serialized length cannot be more than ${MaxUint32}.`);
        const header = block.header;
        if (header.length > MaxUint32) throw new Error(`Block header size cannot be more than ${MaxUint32}.`);
        if (block.type > MaxUint32 || block.type < 0) throw new Error(`Block type must be between O and ${MaxUint32}.`);

        const headerBuff = Buffer.concat([header, Buffer.alloc(4), Buffer.alloc(4), Buffer.alloc(4)]);
        headerBuff.writeUInt32BE(header.length, header.length);
        headerBuff.writeUInt32BE(serialized.length, header.length + 4);
        headerBuff.writeUInt32BE(block.type, header.length + 8);
        const headerHash = HashResolver(headerBuff);
        const headerBuffLength = Buffer.alloc(4);
        headerBuffLength.writeUInt32BE(headerBuff.length);
        const preambleBuff = Buffer.concat([headerHash, headerBuffLength, headerHash, SOB]);
        const finalBuffer = Buffer.concat([serialized, preambleBuff]);

        this.source.append(finalBuffer);
        return finalBuffer.length;
    }

    public * iterateBlocks(validateIntegrity: boolean = false): Generator<IBlock> {

    }

    public defrag() {

    }

    constructor(source: IAppendStore, cacheStrategy: CacheStrategy = CacheStrategy.Default) {
        this.source = source;
        this.cacheStrategy = cacheStrategy;
    }
}

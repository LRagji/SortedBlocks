import { IAppendStore } from "./i-append-store";

export enum CacheStrategy {
    Default, //Should only cache indexes
    None, //Clear cache after every call
    All//Should also cache values
}

export interface IBlockPayload {
    info: Buffer,
    data: Map<bigint, Buffer>
}

export interface IBlockValue {
    key: bigint,
    value: Buffer,
    info: Buffer
}

export interface IBlock {
    info: Buffer,
    version: number,
    startIndex: number,
    endIndex: number,
    validateIntegrity(): boolean
}

export interface KVPBlock extends IBlock {
    minKey: number,
    maxKey: number,
    index: Map<bigint, [startIndex: number, endIndex: number]>,
    get(key: bigint): Buffer
    iterate(ascending: boolean): IterableIterator<[key: bigint, value: Buffer]>
}

export interface DefragBlock extends IBlock {
    DefraggedStart: number
}

export class Blocks {

    public readonly cache = new Array<IBlock>();

    private sourceParsedIndex: number = -1;
    private sourceIndexToStopAt: number = 0;
    private readonly source: IAppendStore;
    private readonly cacheStrategy: CacheStrategy;

    public appendBlock(payload: IBlockPayload) {

    }

    public * iterateBlocks(): Generator<IBlock> {

    }

    public defrag(destination: IAppendStore) {

    }

    constructor(source: IAppendStore, cacheStrategy: CacheStrategy = CacheStrategy.Default) {
        this.source = source;
        this.cacheStrategy = cacheStrategy;
    }
}

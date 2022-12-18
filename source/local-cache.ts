import { IBlocksCache } from "./i-blocks-cache";
import { Block } from "./block";


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

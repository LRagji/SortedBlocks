import { IBlocksCache } from "./i-blocks-cache";
import { Block } from "./block";


export class LocalCache implements IBlocksCache {
    public readonly cache = new Map<string, Block>();

    public get length() {
        return this.cache.size;
    }

    public async set(stringKeyPart: string, absolutePosition: number, block: Block): Promise<void> {
        this.cache.set(this.constructKey(stringKeyPart, absolutePosition), block);
    }
    public async get(stringKeyPart: string, absolutePosition: number): Promise<Block | undefined> {
        return this.cache.get(this.constructKey(stringKeyPart, absolutePosition));
    }
    public async clear(stringKeyPart: string | undefined, before: number | undefined = undefined, after: number | undefined = undefined): Promise<void> {
        this.cache.clear();
    }

    private constructKey(stringKeyPart: string, numericKeyPart: number): string {
        return `${stringKeyPart}-${numericKeyPart}`;
    }

}

import { IBlocksCache } from "./i-blocks-cache";
import { Block } from "./block";


export class LocalCache implements IBlocksCache {
    public readonly cache = new Map<string, Block>();

    public get length() {
        return this.cache.size;
    }

    public set(stringKeyPart: string, absolutePosition: number, block: Block): void {
        this.cache.set(this.constructKey(stringKeyPart, absolutePosition), block);
    }
    public get(stringKeyPart: string, absolutePosition: number): Block | undefined {
        return this.cache.get(this.constructKey(stringKeyPart, absolutePosition));
    }
    public clear(stringKeyPart: string | undefined, before: number | undefined = undefined, after: number | undefined = undefined): void {
        this.cache.clear();
    }

    private constructKey(stringKeyPart: string, numericKeyPart: number): string {
        return `${stringKeyPart}-${numericKeyPart}`;
    }

}

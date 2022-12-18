import { Block } from "./block";

export interface IBlocksCache {
    set(absolutePosition: number, block: Block): void;
    get(absolutePosition: number): Block | undefined;
    clear(before: number | undefined, after: number | undefined): void;
    length: number;
}

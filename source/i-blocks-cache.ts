import { Block } from "./block";

export interface IBlocksCache {
    set(stringKeyPart: string, numericKeyPart: number, block: Block): Promise<void>;
    get(stringKeyPart: string, numericKeyPart: number): Promise<Block | undefined>;
    clear(stringKeyPart: string | undefined, beforeNumericKeyPart: number | undefined, afterNumericKeyPart: number | undefined): Promise<void>;
    length: number;
}

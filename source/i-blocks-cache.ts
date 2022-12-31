import { Block } from "./block";

export interface IBlocksCache {
    set(stringKeyPart: string, numericKeyPart: number, block: Block): void;
    get(stringKeyPart: string, numericKeyPart: number): Block | undefined;
    clear(stringKeyPart: string | undefined, beforeNumericKeyPart: number | undefined, afterNumericKeyPart: number | undefined): void;
    length: number;
}

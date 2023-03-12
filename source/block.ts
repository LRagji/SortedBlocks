import { IAppendStore } from "./i-append-store";
import { MaxUint32 } from "./blocks";


export class Block {
    public type: number = 0;
    public blockPosition: number = -1;
    public blockStartPosition: number = -1;
    public headerLength: number = -1;
    public bodyLength: number = -1;
    public store: IAppendStore | null = null;

    public static from(store: IAppendStore, type: number, blockPosition: number, blockStartPosition: number, headerLength: number, bodyLength: number): Block {
        if (store == null)
            throw new Error(`Parameter "store" cannot be null or undefined.`);
        if (type == null || type < 0 || type > MaxUint32)
            throw new Error(`Parameter "type" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        if (blockPosition == null || blockPosition < 0)
            throw new Error(`Parameter "blockPosition" cannot be null or undefined and has to be greater than 0.`);
        if (headerLength == null || headerLength < 0 || headerLength > MaxUint32)
            throw new Error(`Parameter "headerLength" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        if (bodyLength == null || bodyLength < 0 || bodyLength > MaxUint32)
            throw new Error(`Parameter "bodyLength" cannot be null or undefined and has to be a in range of 0 to ${MaxUint32}.`);
        const returnObject = new Block(type, blockPosition, blockStartPosition);
        returnObject.store = store;
        returnObject.headerLength = headerLength;
        returnObject.bodyLength = bodyLength;
        return returnObject;
    }

    constructor(type: number = 0, blockPosition: number = -1, blockStartPosition: number = -1, private readonly headerBuff: Buffer | undefined = undefined, private readonly bodyBuff: Buffer | undefined = undefined) {
        this.type = type;
        this.blockPosition = blockPosition;
        this.headerLength = headerBuff?.length || -1;
        this.bodyLength = bodyBuff?.length || -1;
        this.blockStartPosition = blockStartPosition;
    }

    public header(): Buffer {
        if (this.headerBuff != undefined) {
            return this.headerBuff;
        }
        else {
            return this.store?.measuredReverseRead(this.blockPosition, this.blockPosition - this.headerLength) || Buffer.alloc(0);
        }
    }
    public body(): Buffer {
        if (this.bodyBuff != undefined) {
            return this.bodyBuff;
        }
        else {
            return this.store?.measuredReverseRead((this.blockPosition - this.headerLength), this.blockPosition - (this.headerLength + this.bodyLength)) || Buffer.alloc(0);
        }
    }
    public merge(other: Block): Block[] {
        throw new Error("Method not implemented.");
    }

}

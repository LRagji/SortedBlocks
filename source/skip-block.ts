import { IAppendStore } from "./i-append-store";
import { SystemBlockTypes } from "./system-block-types";
import { Block } from "./block";

export class SkipBlock extends Block {

    private readonly _header: Buffer;

    constructor(public readonly inclusivePositionFromSkip: bigint, public readonly inclusivePositionToSkip: bigint) {
        super();
        this._header = Buffer.alloc(16);
        this._header.writeBigUint64BE(this.inclusivePositionFromSkip, 0);
        this._header.writeBigUint64BE(this.inclusivePositionToSkip, 8);
        this.store = null;
        this.blockPosition = this._header.length;
        this.bodyLength = 0;
        this.headerLength = this._header.length;
        this.type = SystemBlockTypes.Consolidated;
    }

    public override header(): Buffer {
        return this._header;
    }

    public override body(): Buffer {
        return Buffer.alloc(0);
    }

    public override merge(other: Block): Block {
        throw new Error(`System Block(${this.type}):${this.store?.id} cannot be merged with another Block(${other.type}):${other.store?.id}`);
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): SkipBlock {
        const block = super.from(store, type, blockPosition, headerLength, bodyLength);
        const inclusivePositionFromSkip = block.header().readBigUint64BE(0);
        const inclusivePositionToSkip = block.header().readBigUint64BE(8);
        const returnObject = new SkipBlock(inclusivePositionFromSkip, inclusivePositionToSkip);
        returnObject.blockPosition = blockPosition;
        returnObject.store = store;
        if (returnObject.bodyLength !== bodyLength)
            throw new Error(`Invalid body length ${bodyLength}, must be 0.`);
        return returnObject;
    }

}

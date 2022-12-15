import { Block } from "../../source/blocks";
import { IAppendStore } from "../../source/i-append-store";

export class TestBlock extends Block {
    constructor(private readonly bodyBuff: Buffer, private readonly headerBuff: Buffer, public store: IAppendStore) {
        super();
        this.type = 100;
        this.bodyLength = this.bodyBuff.length;
        this.headerLength = this.headerBuff.length;
    }

    public override header(): Buffer {
        return this.headerBuff;
    }

    public override body(): Buffer {
        return this.bodyBuff;
    }

    public static textBlockFrom(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): TestBlock {
        const b = Block.form(store, type, blockPosition, headerLength, bodyLength);
        return new TestBlock(b.body(), b.header(), store);
    }
}
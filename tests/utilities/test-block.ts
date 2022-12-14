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
}
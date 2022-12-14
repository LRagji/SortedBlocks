import { Block } from "../../source/blocks";

export class TestBlock extends Block {
    constructor(private readonly bodyBuff: Buffer, private readonly headerBuff: Buffer) {
        super();
        this.type = 100;
    }

    public override header(): Buffer {
        return this.headerBuff;
    }

    public override body(): Buffer {
        return this.bodyBuff;
    }
}
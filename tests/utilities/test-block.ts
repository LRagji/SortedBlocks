import { Block } from "../../source/index";
import { IAppendStore } from "../../source/i-append-store";

export class TestBlock extends Block {
    public static type: number = 100;

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

    public override merge(other: Block): Block {
        const header = Buffer.concat([other.header(), this.header()]);
        const body = Buffer.concat([other.body(), this.body()]);
        return new TestBlock(body, header, this.store);
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): TestBlock {
        const b = super.from(store, type, blockPosition, headerLength, bodyLength);
        const returnBlock = new TestBlock(b.body(), b.header(), store);
        returnBlock.blockPosition = blockPosition;
        return returnBlock;
    }
}
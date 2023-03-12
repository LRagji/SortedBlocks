import { Block } from "../../source/index";
import { IAppendStore } from "../../source/i-append-store";

export class TestBlock extends Block {
    public static type: number = 100;

    constructor(bodyBuff: Buffer, headerBuff: Buffer, public override store: IAppendStore) {
        super(100, undefined, undefined, headerBuff, bodyBuff);
    }

    public override merge(other: Block): Block[] {
        const header = Buffer.concat([other.header(), this.header()]);
        const body = Buffer.concat([other.body(), this.body()]);
        return [new TestBlock(body, header, this.store)];
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, blockStartPosittion: number, headerLength: number, bodyLength: number): TestBlock {
        const b = super.from(store, type, blockPosition, blockStartPosittion, headerLength, bodyLength);
        const returnBlock = new TestBlock(b.body(), b.header(), store);
        returnBlock.blockPosition = blockPosition;
        return returnBlock;
    }
}
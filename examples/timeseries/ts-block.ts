import { Block, IAppendStore } from '../../source/index';

export interface ISample {
    tag: number;
    time: bigint;
    value: number;
    reserved: Buffer;
}

export class TSBlock extends Block {

    public push(sample: ISample) {

    }

    public rangeRead(tag: number, fromTime: bigint, toTime: bigint): Array<ISample> {
        throw new Error();
    }

    public static override from(store: IAppendStore, type: number, blockPosition: number, headerLength: number, bodyLength: number): TSBlock {
        throw new Error();
    }

    public override header(): Buffer {
        throw new Error();
    }

    public override body(): Buffer {
        throw new Error();
    }

}
import { IStore } from "../../source/index"
import { IAppendStore } from '../../source/i-append-store';
export class MockedStore implements IStore {

    constructor(public store: string = "", public Id: string = Date.now().toString(), public OptimalBufferSize: number | undefined = undefined) { }

    append(data: string): void {
        this.store += data;
    }
    read(position: number, utf8CharactersToRead: number): string {
        return this.store.substring(position, position + utf8CharactersToRead);
    }
    fetchCurrentSize(): number {
        return this.store.length
    }

}

export class MockedAppendStore implements IAppendStore {

    //Stats
    private readonly readOPS = new Map<number, number>();

    constructor(
        public store: Buffer = Buffer.alloc(0),
        public readCallback = () => 1,
        public Id: string = Date.now().toString(),
        public diagnosticPrint: boolean = false) { }

    reverseRead(fromPosition: number): Buffer {
        const operationTimestamp = Date.now();
        const operationBucket = operationTimestamp - (operationTimestamp % 1000);
        let ops = this.readOPS.get(operationBucket) || 0;
        ops++;
        this.readOPS.set(operationBucket, ops);
        this.readOPS.delete(operationBucket - 15000);

        if (fromPosition < 0) {
            throw new Error(`Param "offset" cannot be lesser than 0.`);
        }
        const length = this.readCallback();
        const start = Math.max(0, fromPosition - length);
        const data = this.store.subarray(start, fromPosition);
        if (this.diagnosticPrint === true) console.log(`read ${start} to ${fromPosition} len ${length}`);
        return data;
    }

    append(data: Buffer): void {
        this.store = Buffer.concat([this.store, data]);
    }

    clear() {
        this.store = Buffer.alloc(0);
        this.readOPS.clear();
    }

    public statistics(): { readOps: Map<number, number> } {
        return { readOps: this.readOPS };
    }
}

export function getRandomInt(min: number, max: number): number {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
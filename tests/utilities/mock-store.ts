import { IAppendStore } from '../../source/i-append-store';

export class MockedAppendStore implements IAppendStore {

    //Stats
    private readonly readOPS = new Map<number, number>();

    constructor(
        public store: Buffer = Buffer.alloc(0),
        public readCallback = () => 1,
        public id: string = Date.now().toString(),
        public diagnosticPrint: boolean = false) { }

    get length() {
        return this.store.length;
    }

    measuredReverseRead(fromInclusive: number, toExclusive: number): Buffer | null {
        let accumulator = Buffer.alloc(0);
        const lengthRequired = (fromInclusive - toExclusive);
        let startPosition = fromInclusive;
        do {
            let data: Buffer | null = this.reverseRead(startPosition);
            if (data != null && data.length !== 0) {
                accumulator = Buffer.concat([data, accumulator]);
                startPosition -= data.length;
            }
            else if (data == null || data.length === 0) {
                break;
            }
        }
        while (lengthRequired > accumulator.length)
        return accumulator.subarray(accumulator.length - lengthRequired);
    }

    reverseRead(fromInclusive: number): Buffer {
        if (fromInclusive >= this.store.length) return Buffer.alloc(0);
        //console.log(`RR:${fromPosition}`);
        const operationTimestamp = Date.now();
        const operationBucket = operationTimestamp - (operationTimestamp % 1000);
        let ops = this.readOPS.get(operationBucket) || 0;
        ops++;
        this.readOPS.set(operationBucket, ops);
        const windowStart = operationBucket - 15000;
        this.readOPS.forEach((v, k) => k < windowStart ? this.readOPS.delete(k) : null);

        if (fromInclusive < 0) {
            throw new Error(`Param "fromPosition" cannot be lesser than 0.`);
        }
        const length = this.readCallback();
        const start = Math.max(0, fromInclusive - length);
        const data = this.store.subarray(start, (fromInclusive + 1));
        if (this.diagnosticPrint === true) console.log(`read ${start} to ${fromInclusive} len ${length}`);
        return data;
    }

    async append(data: Buffer): Promise<void> {
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
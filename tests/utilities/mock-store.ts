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

    constructor(
        public store: Buffer = Buffer.alloc(0),
        public readCallback = () => 1,
        public Id: string = Date.now().toString(),
        public diagnosticPrint: boolean = false) { }

    reverseRead(fromPosition: number): Buffer {
        if (fromPosition < 0) {
            throw new Error(`Param "offset" cannot be lesser than 0.`);
        }
        const length = this.readCallback();
        const start = fromPosition - length;
        const data = this.store.subarray(start, fromPosition);
        if (this.diagnosticPrint === true) console.log(`read ${start} to ${fromPosition} len ${length}`);
        return data;
    }

    append(data: Buffer): void {
        this.store = Buffer.concat([this.store, data]);
    }
}

export function getRandomInt(min: number, max: number): number {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
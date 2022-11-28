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

    constructor(public store: Buffer = Buffer.alloc(0)) { }

    reverseRead(fromPosition: number): Buffer {
        throw new Error("Method not implemented.");
    }

    append(data: Buffer): void {
        this.store = Buffer.concat([this.store, data]);
    }
}
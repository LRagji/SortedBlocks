import { IStore } from "../../source/index"
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
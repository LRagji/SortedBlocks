export interface IAppendStore {
    Id: string;
    OptimalBufferSize: number | undefined
    append(data: Buffer): void;
    read(position: number, utf8CharactersToRead: number): string;
    fetchCurrentSize(): number;
}
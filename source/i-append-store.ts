export interface IAppendStore {
    append(data: Buffer): void;
    reverseRead(fromPosition: number): Buffer | null
}
export interface IAppendStore {
    Id: string;
    append(data: Buffer): void;
    reverseRead(fromPosition: number): Buffer | null
}
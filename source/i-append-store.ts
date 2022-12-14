export interface IAppendStore {
    id: string;
    length: number;
    append(data: Buffer): void;
    reverseRead(fromPosition: number): Buffer | null;
    measuredRead(from: number, to: number): Buffer | null;
}
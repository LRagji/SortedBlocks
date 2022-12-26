export interface IAppendStore {
    id: string;
    length: number;
    append(data: Buffer): Promise<void>;
    reverseRead(fromInclusivePosition: number): Buffer | null;
    measuredReverseRead(fromInclusivePosition: number, toExclusivePosition: number): Buffer | null;
}
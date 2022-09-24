// import { IStore } from ".";

// export class SortedBlocks {
//     constructor(store: IStore) {

//     }

//     public put(data: [key: Buffer, value: Buffer]) {

//     }

//     public get(key: Buffer): Buffer {
//         throw new Error("TBI");
//     }

//     public *iterate(): Generator<[key: Buffer, value: Buffer]> {

//     }
// }


export class SortedSection {
    private index: Buffer = Buffer.allocUnsafe(0);
    private payload: Buffer = Buffer.allocUnsafe(0);
    private keySet = new Set<BigInt>();
    private indexBytePointer = 0;
    private payloadBytePointer = 0;

    public readonly keyWidthInBytes = 8;
    public readonly pointerWidthInBytes = 4;

    constructor(items: number, bytesPerItemValue: number) {
        this.index = Buffer.allocUnsafe(items * (this.keyWidthInBytes + this.pointerWidthInBytes));
        this.payload = Buffer.allocUnsafe(items * bytesPerItemValue);
    }

    add(key: bigint, value: Buffer) {
        if (this.keySet.has(key)) throw new Error(`Cannot add duplicate key ${key}, it already exists.`)
        //Index=Key(64Bit)|DataPacketOffsetFromIndexStart(64Bit)
        this.indexBytePointer = this.index.writeBigInt64BE(key, this.indexBytePointer);
        this.indexBytePointer = this.index.writeUInt32BE(this.payloadBytePointer, this.indexBytePointer);
        this.payloadBytePointer += value.copy(this.payload, this.payloadBytePointer);


        this.keySet.add(key);

    }

    toBuffer(): Buffer {
        const returnValue = Buffer.concat([this.index.subarray(0, this.indexBytePointer), this.payload.subarray(0, this.payloadBytePointer)]);
        this.index = Buffer.allocUnsafe(0);
        this.payload = Buffer.allocUnsafe(0);
        this.indexBytePointer = 0;
        this.payloadBytePointer = 0;
        this.keySet.clear();
        return returnValue;
    }
}
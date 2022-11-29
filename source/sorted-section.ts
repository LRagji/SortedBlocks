export class SortedSection {
    private index: Buffer = Buffer.allocUnsafe(0);
    private payload = Buffer.allocUnsafe(0);
    private keySet = new Set<BigInt>();
    private indexBytePointer = 0;
    private payloadBytePointer = 0;

    public static readonly keyWidthInBytes = 8;
    public static readonly pointerWidthInBytes = 4;

    constructor(items: number, bytesPerItemValue: number) {
        this.index = Buffer.allocUnsafe(items * (SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes));
        this.payload = Buffer.allocUnsafe(items * bytesPerItemValue);
    }

    add(key: bigint, value: Buffer) {
        if (this.keySet.has(key)) throw new Error(`Cannot add duplicate key ${key}, it already exists.`)
        //Index=Key(64Bit)|DataPacketOffsetFromIndexStart(32Bit)
        this.indexBytePointer = this.index.writeBigInt64BE(key, this.indexBytePointer);
        this.indexBytePointer = this.index.writeUInt32BE(this.payloadBytePointer, this.indexBytePointer);
        this.payloadBytePointer += value.copy(this.payload, this.payloadBytePointer);
        this.keySet.add(key);
    }

    toBuffer(): { index: Buffer, values: Buffer } {
        const returnValue = { index: Buffer.from(this.index.subarray(0, this.indexBytePointer)), values: Buffer.from(this.payload.subarray(0, this.payloadBytePointer)) };
        this.index = Buffer.allocUnsafe(0);
        this.payload = Buffer.allocUnsafe(0);
        this.indexBytePointer = 0;
        this.payloadBytePointer = 0;
        this.keySet.clear();
        return returnValue;
    }
}
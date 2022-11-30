export class SortedSection {
    private index: Buffer = Buffer.allocUnsafe(0);
    private payload = Buffer.allocUnsafe(0);
    private keySet = new Set<BigInt>();
    private indexBytePointer = 0;
    private payloadBytePointer = 0;

    public static readonly keyWidthInBytes = 8;
    public static readonly pointerWidthInBytes = 4;

    constructor(items: number, private readonly bytesPerItemValue: number) {
        this.index = Buffer.allocUnsafe(items * (SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes));
        this.payload = Buffer.allocUnsafe(items * this.bytesPerItemValue);
        this.indexBytePointer = this.index.length;
        this.payloadBytePointer = this.payload.length;
    }

    add(key: bigint, value: Buffer): { indexLength: number, payloadLength: number } {
        if (this.keySet.has(key)) throw new Error(`Cannot add duplicate key ${key}, it already exists.`)
        if (value == undefined || value.length > this.bytesPerItemValue) throw new Error(`Value for key:${key} exceeds the max value size specified:${this.bytesPerItemValue}.`)
        //Index=Key(64Bit)|DataPacketOffsetFromIndexStart(32Bit)
        this.indexBytePointer -= SortedSection.keyWidthInBytes;
        this.index.writeBigInt64BE(key, this.indexBytePointer);
        this.indexBytePointer -= SortedSection.pointerWidthInBytes;
        this.index.writeUInt32BE((this.payload.length - this.payloadBytePointer), this.indexBytePointer);
        this.payloadBytePointer -= value.length;
        value.copy(this.payload, this.payloadBytePointer);
        this.keySet.add(key);
        return { indexLength: (this.payload.length - this.indexBytePointer), payloadLength: (this.payload.length - this.payloadBytePointer) };
    }

    toBuffer(): { index: Buffer, values: Buffer } {
        const returnValue = { index: Buffer.from(this.index.subarray(this.indexBytePointer)), values: Buffer.from(this.payload.subarray(this.payloadBytePointer)) };
        this.index = Buffer.allocUnsafe(0);
        this.payload = Buffer.allocUnsafe(0);
        this.indexBytePointer = 0;
        this.payloadBytePointer = 0;
        this.keySet.clear();
        return returnValue;
    }
}
import { IAppendStore } from '../../source/index';
import fs from 'node:fs';
import path from "node:path";

export class FileStore implements IAppendStore {
    id: string;
    private readonly handle: number;
    //private readonly writeCache = Buffer.alloc(1024);
    private readonly readBuffer = Buffer.alloc(1024);
    private readonly cache = new Map<number, Buffer>();

    //Stats
    private readonly readOPS = new Map<number, number>();

    constructor(filePath: string, blockSize: number = 1024) {
        this.id = filePath;
        const directory = path.dirname(filePath);
        fs.mkdirSync(directory, { recursive: true })
        this.handle = fs.openSync(filePath, "as+");
        //this.writeCache = Buffer.alloc(blockSize);
        this.readBuffer = Buffer.alloc(blockSize);
    }

    reverseRead(fromInclusivePosition: number): Buffer | null {
        //Read from cache
        if (this.cache.has(fromInclusivePosition)) {
            // this.keyHitCounter.set(fromOffset, (this.keyHitCounter.get(fromOffset) || 0) + 1);
            return this.cache.get(fromInclusivePosition) as Buffer;
        }
        else if (this.cache.size > 0) {
            //Make sure never to send zero bytes
            const cacheKeys = Array.from(this.cache.keys());
            for (let index = 0; index < cacheKeys.length; index++) {
                const inclusiveStart = cacheKeys[index];
                const value = this.cache.get(inclusiveStart) as Buffer;
                const exclusiveEnd = inclusiveStart - value.length;
                if (fromInclusivePosition <= inclusiveStart && fromInclusivePosition > exclusiveEnd) {
                    const reversePointer = (fromInclusivePosition - exclusiveEnd);
                    return Buffer.from(value.subarray(0, reversePointer));
                }
            }
        }
        console.log(`F:${fromInclusivePosition} T:${fromInclusivePosition - this.readBuffer.length}`)
        //Read from disk
        const operationTimestamp = Date.now();
        const operationBucket = operationTimestamp - (operationTimestamp % 1000);
        let ops = this.readOPS.get(operationBucket) || 0;
        ops++;
        this.readOPS.set(operationBucket, ops);
        this.readOPS.delete(operationBucket - 15000);
        if (fs.readSync(this.handle, this.readBuffer, 0, this.readBuffer.length, (fromInclusivePosition - this.readBuffer.length) + 1) <= 0) {
            return null;
        }

        this.cache.clear();
        this.cache.set(fromInclusivePosition, this.readBuffer);
        return this.readBuffer;

    }

    public measuredReverseRead(fromInclusivePosition: number, toExclusivePosition: number): Buffer | null {
        let accumulator = Buffer.alloc(0);
        const lengthRequired = (fromInclusivePosition - toExclusivePosition);
        let startPosition = fromInclusivePosition;
        do {
            let data: Buffer | null = this.reverseRead(startPosition);
            if (data != null && data.length !== 0) {
                accumulator = Buffer.concat([data, accumulator]);
                startPosition -= data.length;
            }
            else if (data == null || data.length === 0) {
                break;
            }
        }
        while (lengthRequired > accumulator.length)
        return accumulator.subarray(accumulator.length - lengthRequired);
    }

    public append(data: Buffer): void {
        fs.writeSync(this.handle, data, 0, data.length);
    }

    public get length(): number {
        const size = fs.statSync(this.id).size;
        return size;
    }

    public close() {
        fs.closeSync(this.handle);
    }

    public statistics(): { readOps: Map<number, number> } {
        return { readOps: this.readOPS };
    }
}
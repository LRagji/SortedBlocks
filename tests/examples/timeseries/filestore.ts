import { IAppendStore } from '../../../source/i-append-store.js';
import fs from 'node:fs';
import path from "node:path";

export class FileStore implements IAppendStore {
    Id: string;
    private readonly handle: number;
    //private readonly writeCache = Buffer.alloc(1024);
    private readonly readCache = Buffer.alloc(1024);

    constructor(filePath: string, blockSize: number = 1024) {
        this.Id = filePath;
        const directory = path.dirname(filePath);
        fs.mkdirSync(directory, { recursive: true })
        this.handle = fs.openSync(filePath, "as+");
        //this.writeCache = Buffer.alloc(blockSize);
        this.readCache = Buffer.alloc(blockSize);
    }

    public append(data: Buffer): void {
        fs.writeSync(this.handle, data, 0, data.length);
    }

    public reverseRead(fromPosition: number): Buffer | null {
        if (fs.readSync(this.handle, this.readCache, 0, this.readCache.length, fromPosition - this.readCache.length) > 0) {
            return this.readCache;
        }
        else {
            return null;
        }
    }

    public size(): number {
        const size = fs.statSync(this.Id).size;
        return size;
    }

    public close() {
        fs.closeSync(this.handle);
    }
}
import { IAppendStore } from "./i-append-store";

export interface ICacheProxy {
    reverseRead(fromOffset: number, store: IAppendStore, cacheProbability: number): Buffer | null;
    clear(): void;
}

export class LocalCache implements ICacheProxy {

    private readonly cache = new Map<number, Buffer>();

    constructor(
        private readonly maxEntriesToCache: number = 1000,
        private readonly minBytesForEntryToCache: number = 3) {
        if (this.maxEntriesToCache < 0) throw new Error(`Paramater "maxEntriesToCache" has to be a positive number.`);

        if (this.minBytesForEntryToCache < 1) throw new Error(`Paramater "minBytesForEntryToCache" has to be a positive number greater than zero.`);
    }

    clear(): void {
        this.cache.clear();
    }

    reverseRead(fromOffset: number, store: IAppendStore, cacheProbability: number = 0): Buffer | null {
        if (this.cache.has(fromOffset)) {
            return this.cache.get(fromOffset) as Buffer;
        }
        else if (this.cache.size > 0) {
            //Make sure never to send zero bytes
            const cacheKeys = Array.from(this.cache.keys());
            for (let index = 0; index < cacheKeys.length; index++) {
                const key = cacheKeys[index];
                const value = this.cache.get(key) as Buffer;
                const endPosition = key - value.length;
                if (fromOffset <= key && fromOffset > endPosition) {
                    const reversePointer = fromOffset - endPosition;
                    return Buffer.from(value.subarray(0, reversePointer));
                }
            }
        }
        const data = store.reverseRead(fromOffset);
        if (data != null && data.length > this.minBytesForEntryToCache && cacheProbability > 0) {
            //Push out old data 
            this.cache.set(fromOffset, data);
        }
        return data;
    }

}
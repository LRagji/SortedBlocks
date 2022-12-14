import { IAppendStore } from "./i-append-store";

export interface ICacheProxy {
    reverseRead(fromOffset: number, store: IAppendStore, cacheProbability: number): Buffer | null;
    clear(): void;
}

export class LocalCache implements ICacheProxy {

    private readonly cache = new Map<number, Buffer>();
    private readonly cacheWeightExpiry = new Map<number, Map<number, number>>();//weight=>offset=>datalength
    private readonly keyHitCounter = new Map<number, number>();//offset=>hits

    constructor(
        private readonly maxEntriesToCache: number = 1000,
        private readonly minBytesForEntryToCache: number = 3) {
        if (this.maxEntriesToCache < 0) throw new Error(`Paramater "maxEntriesToCache" has to be a positive number.`);

        if (this.minBytesForEntryToCache < 1) throw new Error(`Paramater "minBytesForEntryToCache" has to be a positive number greater than zero.`);
    }

    public clear(): void {
        this.cache.clear();
        this.cacheWeightExpiry.clear();
        this.keyHitCounter.clear();
    }

    public statistics(): Map<number, { Bytes: number, Count: number, Hits: number }> {
        let returnObject = new Map<number, { Bytes: number, Count: number, Hits: number }>();
        this.cacheWeightExpiry.forEach((v, k) => {
            const statObject = { "Bytes": 0, "Count": 0, "Hits": 0 };
            v.forEach((bytes, offsetKey) => {
                statObject.Bytes += bytes;
                statObject.Count += 1;
                statObject.Hits += this.keyHitCounter.get(offsetKey) || 0
            })
            returnObject.set(k, statObject);
        });
        return returnObject;
    }

    public reverseRead(fromOffset: number, store: IAppendStore, cacheWeight: number = 0): Buffer | null {
        //Check if data exists
        if (this.cache.has(fromOffset)) {
            this.keyHitCounter.set(fromOffset, (this.keyHitCounter.get(fromOffset) || 0) + 1);
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
        //Read actual data
        const data = store.reverseRead(fromOffset);
        //Set data
        if (data != null && data.length > this.minBytesForEntryToCache && cacheWeight > 0) {
            //Clear old data if needed
            if (this.cache.size > this.maxEntriesToCache) {
                const weightClassToExpire = Float64Array.from(this.cacheWeightExpiry.keys()).sort()[0];
                const keyToByteLengths = this.cacheWeightExpiry.get(weightClassToExpire);
                if (keyToByteLengths != null) {
                    let maxBytesminHits = Number.MIN_VALUE, keyToRemove = 0;
                    const keyToBytesIterator = keyToByteLengths.entries();
                    let result = keyToBytesIterator.next();
                    while (!result.done) {
                        const key = result.value[0];
                        const bytes = result.value[1];
                        const hits = Math.max(this.keyHitCounter.get(key) || 1, 1);
                        const ratio = bytes / hits;
                        if (ratio > maxBytesminHits) {
                            maxBytesminHits = ratio;
                            keyToRemove = key;
                        }
                        result = keyToBytesIterator.next();
                    }
                    keyToByteLengths.delete(keyToRemove);
                    this.cache.delete(keyToRemove);
                    this.keyHitCounter.delete(keyToRemove);
                }
            }
            this.cache.set(fromOffset, data);
            const weightClass = this.cacheWeightExpiry.get(cacheWeight) || new Map<number, number>();
            weightClass.set(fromOffset, data.length);
            this.cacheWeightExpiry.set(cacheWeight, weightClass);
        }
        return data;
    }

}
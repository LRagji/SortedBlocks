import * as assert from 'node:assert';
import { ICacheProxy, LocalCache } from '../../source/cache-proxy';
import { Version1SortedBlocks } from '../../source/sorted-blocks';
import { MockedAppendStore, getRandomInt } from '../utilities/mock-store';

const version = Buffer.alloc(1);
const bucketFactor = BigInt(1024);
version.writeUIntBE(1, 0, 1);
const modes: Array<{ name: string, context: { store: MockedAppendStore, cache: ICacheProxy | null } }> = [
    // { name: "Read Single Byte", context: { store: new MockedAppendStore(), cache: new LocalCache() } },
    // { name: "Read Random Bytes", context: { store: new MockedAppendStore(undefined, () => getRandomInt(1, 10), undefined), cache: new LocalCache() } },
    // { name: "Read Fixed Bytes", context: { store: new MockedAppendStore(undefined, () => 1024), cache: new LocalCache() } },
    { name: "Read Fixed Without Cache", context: { store: new MockedAppendStore(undefined, () => 1024), cache: null } },
];

while (modes.length > 0) {
    const mode = modes.shift();
    if (mode == null) {
        throw new Error("Mode cannot be empty.");
    }
    describe(`[${mode.name}]sorted-section serialize/deserialize specs`, () => {

        beforeEach(async function () {
            mode.context.store.clear();
            mode.context.cache?.clear();
        });

        afterEach(async function () {
            const stats = mode.context.store.statistics();
            const readStats = Array.from(stats.readOps.values()).reduce((acc, e) => {
                acc.max = Math.max(acc.max, e);
                acc.min = Math.min(acc.min, e);
                acc.sum = acc.sum + e;
                return acc;
            }, { min: Number.MAX_SAFE_INTEGER, max: Number.MIN_SAFE_INTEGER, sum: 0 });
            console.log(`Read IOPS: Max:${readStats.max} Min:${readStats.min} Total:${readStats.sum} Avg:${readStats.sum / stats.readOps.size}/sec`)
        });

        it('shoud not be able to serialize empty payload', async () => {
            const mockStore = mode.context.store;
            const blockInfo = "1526919030474-55";
            const blockInfoBuff = Buffer.from(blockInfo);

            assert.throws(() => Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>()), new Error(`Payload cannot be empty map`));
        })

        it('should allow blockinfo to be empty buffer', async () => {
            const mockStore = mode.context.store;
            const content = "Hello World String";
            const key = BigInt(102), value = Buffer.from(content), blockInfoBuff = Buffer.alloc(0);

            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length, mode.context.cache);
            if (sortedBlock == null) assert.fail("sortedBlock cannot be null");
            assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
            assert.strictEqual(sortedBlock.meta.keyRangeMax, key);
            assert.strictEqual(sortedBlock.meta.keyRangeMin, key);
            assert.strictEqual(sortedBlock.meta.keyBucketFactor, bucketFactor);
            assert.deepEqual(sortedBlock.meta.blockInfo, blockInfoBuff);

            const retrivedValue = sortedBlock.get(key);
            assert.deepStrictEqual(retrivedValue, value);
        })

        it('shoud be able to read null when key doesnt exists', async () => {
            const mockStore = mode.context.store;
            const content = "Hello World String";
            const blockInfo = "1526919030474-55";
            const key = BigInt(102), value = Buffer.from(content), blockInfoBuff = Buffer.from(blockInfo);

            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length, mode.context.cache);
            if (sortedBlock == null) assert.fail("sortedBlock cannot be null");

            let retrivedValue = sortedBlock.get(BigInt(10));
            assert.deepStrictEqual(retrivedValue, null);

            retrivedValue = sortedBlock.get(BigInt(4849244555433));
            assert.deepStrictEqual(retrivedValue, null);
        })

        it('shoud be able to serialize same key in different blocks and deserialize both', async () => {
            const mockStore = mode.context.store;
            const content = "Hello World String";
            const key = BigInt(102), value = Buffer.from(content), blocks = [Buffer.from("Block1"), Buffer.from("Block2")];

            const bytesWritten = blocks.reduce((acc, b) => {
                return acc + Version1SortedBlocks.serialize(mockStore, b, new Map<bigint, Buffer>([[key, value]]), value.length);
            }, 0);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);

            let scanFrom = mockStore.store.length;
            blocks.reverse().forEach((expectedBlockInfo, idx) => {
                let sortedBlock = Version1SortedBlocks.deserialize(mockStore, scanFrom, mode.context.cache);
                if (sortedBlock == null) assert.fail(`SortedBlock cannot be null for index:${idx}`);
                assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
                assert.strictEqual(sortedBlock.meta.keyRangeMax, key);
                assert.strictEqual(sortedBlock.meta.keyRangeMin, key);
                assert.strictEqual(sortedBlock.meta.keyBucketFactor, bucketFactor);
                assert.deepEqual(sortedBlock.meta.blockInfo, expectedBlockInfo);
                scanFrom = sortedBlock.meta.nextBlockOffset;
                const retrivedValue = sortedBlock.get(key);
                assert.deepStrictEqual(retrivedValue, value);
            });
        })

        it('shoud be able to serialize/deserialize single kvp data to its original form', async () => {
            const mockStore = mode.context.store;
            const content = "Hello World String";
            const blockInfo = "1526919030474-55";
            const key = BigInt(102), value = Buffer.from(content), blockInfoBuff = Buffer.from(blockInfo);

            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length, mode.context.cache);
            if (sortedBlock == null) assert.fail("sortedBlock cannot be null");
            assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
            assert.strictEqual(sortedBlock.meta.keyRangeMax, key);
            assert.strictEqual(sortedBlock.meta.keyRangeMin, key);
            assert.strictEqual(sortedBlock.meta.keyBucketFactor, bucketFactor);
            assert.deepEqual(sortedBlock.meta.blockInfo, blockInfoBuff);
            assert.strictEqual(sortedBlock.meta.actualHeaderStartPosition, 172);
            assert.strictEqual(sortedBlock.meta.actualHeaderEndPosition, 50);

            const retrivedValue = sortedBlock.get(key);
            assert.deepStrictEqual(retrivedValue, value);
        })

        it('shoud be able to serialize/deserialize multiple kvp data to its original form ', async () => {
            const mockStore = mode.context.store;
            const content = "Hello World String";
            const blockInfo = "1526919030474-55";
            const blockInfoBuff = Buffer.from(blockInfo);
            const numberOfSamples = 1000;

            const kvps = new Map<bigint, Buffer>();
            for (let idx = 0; idx < numberOfSamples; idx++) {
                kvps.set(BigInt(idx), Buffer.from(content + `${idx}.`))
            }

            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, kvps);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);
            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length, mode.context.cache);
            if (sortedBlock == null) assert.fail("sortedBlock cannot be null");
            assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
            assert.strictEqual(sortedBlock.meta.keyRangeMax, BigInt(numberOfSamples - 1));
            assert.strictEqual(sortedBlock.meta.keyRangeMin, BigInt(0));
            assert.strictEqual(sortedBlock.meta.keyBucketFactor, bucketFactor);
            assert.deepEqual(sortedBlock.meta.blockInfo, blockInfoBuff);

            for (let idx = 0; idx < numberOfSamples; idx++) {
                const key = BigInt(idx);
                const retrivedValue: Buffer | null = sortedBlock.get(key);
                if (retrivedValue == null) {
                    assert.fail(`Value for key${idx} cannot be null`);
                }
                else {
                    assert.deepStrictEqual(retrivedValue, kvps.get(key), `Values not equal for key:${key}}`);
                }
            }
        }).timeout(-1)

        it('should serialize 1 million key value pairs in acceptable time', async () => {
            const numberOfValues = 1000000;
            const mockStore = mode.context.store;
            const content = "____________This is a test content for 61 bytes._____________";
            const blockInfo = "1526919030474-55";
            const blockInfoBuff = Buffer.from(blockInfo);
            const value = Buffer.from(content);

            const payload = new Map<bigint, Buffer>();
            for (let index = 0; index < numberOfValues; index++) {
                payload.set(BigInt(index), value);
            }

            let st = Date.now();
            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, payload, value.length);
            let elapsed = Date.now() - st;
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);

            console.log(`Serializing[${elapsed}ms] ${numberOfValues} items of ${value.length + 8} bytes each results in ${((bytesWritten / 1024) / 1024).toFixed(2)} MB, Overhead: ${((((bytesWritten) - ((value.length + 8) * numberOfValues)) / ((value.length + 8) * numberOfValues)) * 100).toFixed(2)}%.`)

            const readKey = BigInt(353213);//BigInt(getRandomInt(0, numberOfValues)); //This can be any number to level field across modes i have to fix this to one or ops will vary.
            st = Date.now();
            const block = Version1SortedBlocks.deserialize(mockStore, bytesWritten, mode.context.cache);
            const actualValue = block?.get(readKey);
            elapsed = Date.now() - st;
            assert.notEqual(block, null);
            assert.deepEqual(actualValue, payload.get(readKey));
            const stats = mockStore.statistics();
            const readStats = Array.from(stats.readOps.values()).reduce((acc, e) => {
                acc.max = Math.max(acc.max, e);
                acc.min = Math.min(acc.min, e);
                acc.sum = acc.sum + e;
                return acc;
            }, { min: Number.MAX_SAFE_INTEGER, max: Number.MIN_SAFE_INTEGER, sum: 0 });
            console.log(`Deserialize & Get Time[key:${readKey}]: ${elapsed}ms, IOPS: Max:${readStats.max} Min:${readStats.min} Total:${readStats.sum} Avg:${readStats.sum / stats.readOps.size}/sec`)

        }).timeout(-1)
    });
}
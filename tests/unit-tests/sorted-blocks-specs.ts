import * as assert from 'node:assert';
import crypto from 'node:crypto';
import { Version1SortedBlocks } from '../../source/sorted-blocks';
import { MockedAppendStore, getRandomInt } from '../utilities/mock-store';

const version = Buffer.alloc(1);
const bucketFactor = BigInt(1024);
version.writeUIntBE(1, 0, 1);
const modes: Array<{ name: string, context: { store: MockedAppendStore } }> = [
    { name: "Read Single Byte", context: { store: new MockedAppendStore() } },
    { name: "Read Random Bytes", context: { store: new MockedAppendStore(undefined, () => getRandomInt(1, 10), undefined, true) } },
    { name: "Read Fixed Bytes", context: { store: new MockedAppendStore(undefined, () => 1024) } },
];

while (modes.length > 0) {
    const mode = modes.shift();
    if (mode == null) {
        throw new Error("Mode cannot be empty.");
    }
    describe(`[${mode.name}]sorted-section serialize/deserialize specs`, () => {

        beforeEach(async function () {
            mode.context.store.clear();
        });

        afterEach(async function () {

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

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
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

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
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
                let sortedBlock = Version1SortedBlocks.deserialize(mockStore, scanFrom);
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

            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
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
            const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
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
                    assert.deepStrictEqual(retrivedValue, kvps.get(key));
                }
            }
        })

        it('should serialize 1 million data points in acceptable time', async () => {
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

            const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, payload, value.length);
            assert.deepStrictEqual(bytesWritten, mockStore.store.length);
            console.log(`${numberOfValues} items of ${value.length + 8} bytes each results in ${((bytesWritten / 1024) / 1024).toFixed(2)} MB, Overhead: ${((((bytesWritten) - ((value.length + 8) * numberOfValues)) / ((value.length + 8) * numberOfValues)) * 100).toFixed(2)}%.`)
        }).timeout(-1)
    });
}
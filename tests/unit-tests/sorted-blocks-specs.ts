import * as assert from 'node:assert';
import crypto from 'node:crypto';
import { Version1SortedBlocks } from '../../source/sorted-blocks';
import { MockedAppendStore, getRandomInt } from '../utilities/mock-store';

const hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
const magicBuffer = hashResolver(Buffer.from(`16111987`));
const SOP = magicBuffer.subarray(0, 4);;
const EOP = magicBuffer.subarray(12, 16);
const version = Buffer.alloc(1);
const bucketFactor = 1024;
version.writeUIntBE(1, 0, 1);

describe(`sorted-section write specs`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('shoud be able to serialize and deserialize data to its original form when reading single bytes from store underneath', async () => {
        const mockStore = new MockedAppendStore();
        const content = "Hello World String";
        const blockInfo = "1526919030474-55";
        const key = BigInt(1), value = Buffer.from(content), blockInfoBuff = Buffer.from(blockInfo);

        const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);
        assert.deepStrictEqual(bytesWritten, mockStore.store.length);

        const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
        if (sortedBlock == null) assert.fail("sortedBlock cannot be null");
        assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
        assert.strictEqual(sortedBlock.meta.keyRangeMax, key);
        assert.strictEqual(sortedBlock.meta.keyRangeMin, key);
        assert.strictEqual(sortedBlock.meta.keyBucketFactor, BigInt(1024));
        assert.deepEqual(sortedBlock.meta.blockInfo, blockInfoBuff);
        assert.strictEqual(sortedBlock.meta.actualHeaderStartPosition, 164);
        assert.strictEqual(sortedBlock.meta.actualHeaderEndPosition, 42);

        //console.log(sortedBlock.get(key));
    })

    it('shoud be able to serialize and deserialize data to its original form when reading random length bytes from store underneath', async () => {
        const mockStore = new MockedAppendStore(undefined, () => getRandomInt(1, 10), undefined, true);
        //const debugArray = [9, 5, 2, 4, 1, 1, 4, 4, 1, 5, 1, 1, 9, 6, 8, 1, 2, 9, 5, 1, 9, 2, 8, 10, 6, 9, 9, 7, 9, 2, 9, 2, 6];
        //const mockStore = new MockedAppendStore(undefined, () => debugArray.shift() as number, undefined, false);
        const content = "Hello World String";
        const blockInfo = "1526919030474-55";
        const key = BigInt(1), value = Buffer.from(content), blockInfoBuff = Buffer.from(blockInfo);

        const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);
        assert.deepStrictEqual(bytesWritten, mockStore.store.length);

        const sortedBlock = Version1SortedBlocks.deserialize(mockStore, mockStore.store.length);
        if (sortedBlock == null) assert.fail("sortedBlock cannot be null");
        assert.strictEqual(sortedBlock.meta.storeId, mockStore.Id);
        assert.strictEqual(sortedBlock.meta.keyRangeMax, key);
        assert.strictEqual(sortedBlock.meta.keyRangeMin, key);
        assert.strictEqual(sortedBlock.meta.keyBucketFactor, BigInt(1024));
        assert.deepEqual(sortedBlock.meta.blockInfo, blockInfoBuff);
        assert.strictEqual(sortedBlock.meta.actualHeaderStartPosition, 164);
        assert.strictEqual(sortedBlock.meta.actualHeaderEndPosition, 42);
    })

    // it('should throw if key is already presented', async () => {
    //     const content = "Hello World String";
    //     const target = new SortedSection(1, content.length);
    //     const key = BigInt(1), value = Buffer.from(content);
    //     target.add(key, value);
    //     assert.throws(() => target.add(key, value), new Error(`Cannot add duplicate key ${key}, it already exists.`))
    // })

    it('should serialize 1 million data points in acceptable time', async () => {
        const numberOfValues = 1000000;
        const mockStore = new MockedAppendStore();
        const content = "Hello World String";
        const blockInfo = "1526919030474-55";
        const blockInfoBuff = Buffer.from(blockInfo);
        const value = Buffer.from(content);

        const payload = new Map<bigint, Buffer>();
        for (let index = 0; index < numberOfValues; index++) {
            payload.set(BigInt(index), value);
        }

        const bytesWritten = Version1SortedBlocks.serialize(mockStore, blockInfoBuff, payload, value.length);
        assert.deepStrictEqual(bytesWritten, mockStore.store.length);
        console.log(`1 Million blocksize ${bytesWritten} Bytes with ${value.length} Bytes of payload.`)
    }).timeout(-1)
});
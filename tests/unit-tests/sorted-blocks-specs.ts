import * as assert from 'node:assert';
import crypto from 'node:crypto';
import { Version1SortedBlocks } from '../../source/sorted-blocks';
import { MockedAppendStore } from '../utilities/mock-store';

const hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();
const SOP = Version1SortedBlocks.SOP;
const EOP = Version1SortedBlocks.EOP;
const version = Buffer.alloc(4);
const bucketFactor = 1024;
version.writeUInt32BE(1);

describe(`sorted-section write specs`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should be doing correct data assembly', async () => {
        const mockStore = new MockedAppendStore();
        const content = "Hello World String";
        const blockInfo = "1526919030474-55";
        const target = new Version1SortedBlocks(mockStore);
        const key = BigInt(1), value = Buffer.from(content), blockInfoBuff = Buffer.from(blockInfo);

        const bytesReturned = target.put(blockInfoBuff, new Map<bigint, Buffer>([[key, value]]), value.length);

        assert.deepStrictEqual(bytesReturned, mockStore.store.length);
        let start = SOP.length * -1, end = 0;//SOP
        assert.deepStrictEqual(mockStore.store.subarray(start), SOP);
        end = start; start -= version.length;//Version
        assert.deepStrictEqual(mockStore.store.subarray(start, end), version);
        end = start; start -= 4;//Header Length 1
        const headerLength1 = mockStore.store.subarray(start, end).readUInt32BE(0);
        assert.deepStrictEqual(headerLength1 > 0 && headerLength1 < bytesReturned, true);
        end = start; start -= 16;//Header Hash 1
        const headerHash1 = mockStore.store.subarray(start, end);
        end = start; start -= 4;//Header Length 2
        const headerLength2 = mockStore.store.subarray(start, end).readUInt32BE(0);
        assert.deepStrictEqual(headerLength2 > 0 && headerLength2 < bytesReturned, true);
        assert.deepStrictEqual(headerLength1, headerLength2);
        end = start; start -= 16;//Header Hash 2
        const headerHash2 = mockStore.store.subarray(start, end);
        assert.deepStrictEqual(headerHash1, headerHash2);
        const hashedHeader = mockStore.store.subarray(start - headerLength1, start);//Hashed Header
        assert.deepStrictEqual(headerHash1, hashResolver(hashedHeader));
        end = start; start -= 4; //Index Length
        const indexLength = mockStore.store.subarray(start, end).readUint32BE(0);
        end = start; start -= 4; //Data Length
        const dataLength = mockStore.store.subarray(start, end).readUint32BE(0);
        end = start; start -= 4; //BlockInfo Length
        const blockInfoLength = mockStore.store.subarray(start, end).readUint32BE(0);
        end = start; start -= blockInfoLength; //BlockInfo
        const aBlockInfo = mockStore.store.subarray(start, end).toString();
        assert.deepStrictEqual(aBlockInfo, blockInfo);
        end = start; start -= 16; //Index Hash
        const IndexHash = mockStore.store.subarray(start, end);
        end = start; start -= 16; //Data Length
        const dataHash = mockStore.store.subarray(start, end);
        end = start; start -= 4; //Bucket Factor
        const abucketFactor = mockStore.store.subarray(start, end).readUint32BE(0);
        assert.deepEqual(abucketFactor, bucketFactor);
        end = start; start -= 8; //Max ID
        const maxId = mockStore.store.subarray(start, end).readBigInt64BE(0);
        assert.deepEqual(maxId, key);
        end = start; start -= 8; //Min ID
        const minId = mockStore.store.subarray(start, end).readBigInt64BE(0);
        assert.deepEqual(minId, key);
        end = start; start -= 16; //EOP
        const aEOP = mockStore.store.subarray(start, end);
        assert.deepEqual(aEOP, EOP);
        const aIndex = mockStore.store.subarray(start - indexLength, start);//Hashed Index
        assert.deepStrictEqual(IndexHash, hashResolver(aIndex));
        const aData = mockStore.store.subarray((start - indexLength) - dataLength, (start - indexLength));//Hashed Data
        assert.deepStrictEqual(dataHash, hashResolver(aData));
        //Section Index


    })

    // it('should throw if key is already presented', async () => {
    //     const content = "Hello World String";
    //     const target = new SortedSection(1, content.length);
    //     const key = BigInt(1), value = Buffer.from(content);
    //     target.add(key, value);
    //     assert.throws(() => target.add(key, value), new Error(`Cannot add duplicate key ${key}, it already exists.`))
    // })

    it('should be doing correct data assembly with multiple values', async () => {
        const numberOfValues = 1000000;
        const mockStore = new MockedAppendStore();
        const content = "Hello World String";
        const blockInfo = "1526919030474-55";
        const blockInfoBuff = Buffer.from(blockInfo);
        const target = new Version1SortedBlocks(mockStore);
        const value = Buffer.from(content);

        const payload = new Map<bigint, Buffer>();
        for (let index = 0; index < numberOfValues; index++) {
            payload.set(BigInt(index), value);
        }

        const bytesWritten = target.put(blockInfoBuff, payload, value.length);

        console.log(bytesWritten);
    }).timeout(-1)
});
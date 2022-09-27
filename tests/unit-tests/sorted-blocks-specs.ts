import * as assert from 'node:assert';
import crypto from 'node:crypto';
import { Version1SortedBlocks } from '../../source/sorted-blocks';
import { MockedAppendStore } from '../utilities/mock-store';

const hashResolver = (serializedData: Buffer) => crypto.createHash('md5').update(serializedData).digest();


describe(`sorted-section write specs`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should be doing correct data assembly', async () => {
        const mockStore = new MockedAppendStore();
        const content = "Hello World String";
        const blockId = "1526919030474-55";
        const target = new Version1SortedBlocks(mockStore);
        const key = BigInt(1), value = Buffer.from(content), blockIdBuff = Buffer.from(blockId);
        const bytesReturned = target.put(blockIdBuff, new BigInt64Array([key]), [value], value.length);
        assert.deepStrictEqual(bytesReturned, mockStore.store.length);
    })

    // it('should throw if key is already presented', async () => {
    //     const content = "Hello World String";
    //     const target = new SortedSection(1, content.length);
    //     const key = BigInt(1), value = Buffer.from(content);
    //     target.add(key, value);
    //     assert.throws(() => target.add(key, value), new Error(`Cannot add duplicate key ${key}, it already exists.`))
    // })

    // it('should be doing correct indexing with multiple values', async () => {
    //     const numberOfValues = 1000000;
    //     const content = "Hello World String";
    //     const target = new SortedSection(numberOfValues, content.length);
    //     const value = Buffer.from(content);
    //     for (let index = 0; index < numberOfValues; index++) {
    //         target.add(BigInt(index), value);
    //     }
    //     const iResult = target.toBuffer();
    //     const result = Buffer.concat([iResult.index, iResult.values]);
    //     const expectedByteLength = (8 + 4 + value.length) * numberOfValues;
    //     let offset = 0;
    //     const valueBaseOffset = numberOfValues * (8 + 4);
    //     assert.deepStrictEqual(expectedByteLength, result.length);
    //     for (let index = 0; index < numberOfValues; index++) {
    //         const keyBaseOffset = index * (8 + 4);
    //         const key = BigInt(index);
    //         assert.deepStrictEqual(key, result.readBigInt64BE(keyBaseOffset));
    //         assert.deepStrictEqual(offset, result.readUInt32BE(keyBaseOffset + 8));
    //         assert.deepStrictEqual(content, result.toString("utf8", (valueBaseOffset + offset), (valueBaseOffset + offset) + content.length));
    //         offset += value.length;
    //     }
    // })
});
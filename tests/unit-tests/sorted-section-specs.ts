import * as assert from 'node:assert';
import { SortedSection } from '../../source/sorted-blocks';

const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));


describe(`sorted-section read specs`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should be doing correct indexing', async () => {
        const content = "Hello World String";
        const target = new SortedSection(1, content.length);
        const key = BigInt(1), value = Buffer.from(content);
        target.add(key, value);
        const iResult = target.toBuffer();
        const result = Buffer.concat([iResult.index, iResult.values]);
        const expectedByteLength = 8 + 4 + value.length;
        const offset = 0;
        assert.deepStrictEqual(expectedByteLength, result.length);
        assert.deepStrictEqual(key, result.readBigInt64BE(0));
        assert.deepStrictEqual(offset, result.readUInt32BE(8));
        assert.deepStrictEqual(content, result.toString("utf8", 8 + 4));
    })

    it('should throw if key is already presented', async () => {
        const content = "Hello World String";
        const target = new SortedSection(1, content.length);
        const key = BigInt(1), value = Buffer.from(content);
        target.add(key, value);
        assert.throws(() => target.add(key, value), new Error(`Cannot add duplicate key ${key}, it already exists.`))
    })

    it('should be doing correct indexing with multiple values', async () => {
        const numberOfValues = 1000000;
        const content = "Hello World String";
        const target = new SortedSection(numberOfValues, content.length);
        const value = Buffer.from(content);
        for (let index = 0; index < numberOfValues; index++) {
            target.add(BigInt(index), value);
        }
        const iResult = target.toBuffer();
        const result = Buffer.concat([iResult.index, iResult.values]);
        const expectedByteLength = (8 + 4 + value.length) * numberOfValues;
        let offset = 0;
        const valueBaseOffset = numberOfValues * (8 + 4);
        assert.deepStrictEqual(expectedByteLength, result.length);
        for (let index = 0; index < numberOfValues; index++) {
            const keyBaseOffset = index * (8 + 4);
            const key = BigInt(index);
            assert.deepStrictEqual(key, result.readBigInt64BE(keyBaseOffset));
            assert.deepStrictEqual(offset, result.readUInt32BE(keyBaseOffset + 8));
            assert.deepStrictEqual(content, result.toString("utf8", (valueBaseOffset + offset), (valueBaseOffset + offset) + content.length));
            offset += value.length;
        }
    })
});
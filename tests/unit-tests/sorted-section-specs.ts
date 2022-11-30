import * as assert from 'node:assert';
import { SortedSection } from '../../source/sorted-section';



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
        const result = Buffer.concat([iResult.values, iResult.index]);
        const expectedByteLength = SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes + value.length;
        let readingOffset = expectedByteLength;
        const indexOffset = 0;
        assert.deepStrictEqual(expectedByteLength, result.length);
        readingOffset -= SortedSection.keyWidthInBytes;
        assert.deepStrictEqual(key, result.readBigInt64BE(readingOffset));
        readingOffset -= SortedSection.pointerWidthInBytes;
        assert.deepStrictEqual(indexOffset, result.readUInt32BE(readingOffset));
        assert.deepStrictEqual(value, result.subarray(0, readingOffset));
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
        const result = Buffer.concat([iResult.values, iResult.index,]);
        const expectedByteLength = (SortedSection.keyWidthInBytes + SortedSection.pointerWidthInBytes + value.length) * numberOfValues;
        assert.deepStrictEqual(expectedByteLength, result.length);

        let keyBaseOffset = expectedByteLength;
        for (let index = 0; index < numberOfValues; index++) {
            const key = BigInt(index);
            keyBaseOffset -= SortedSection.keyWidthInBytes;
            assert.deepStrictEqual(key, result.readBigInt64BE(keyBaseOffset));
            keyBaseOffset -= SortedSection.pointerWidthInBytes;
            const valueOffset = result.readUInt32BE(keyBaseOffset);
            const indexedOffset = result.length - (valueOffset + iResult.index.length);
            assert.deepStrictEqual(value, result.subarray(indexedOffset - value.length, indexedOffset));
        }
    }).timeout(-1)
});
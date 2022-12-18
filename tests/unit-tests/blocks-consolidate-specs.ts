import * as assert from 'node:assert';
import { Block, Blocks, CachePolicy } from '../../source/blocks';
import { MockedAppendStore } from '../utilities/mock-store';
import { TestBlock } from '../utilities/test-block';
import sinon, { SinonSpiedInstance } from 'sinon';


describe(`Blocks consolidate specs`, () => {
    const sandbox = sinon.createSandbox();
    let mockStore: SinonSpiedInstance<MockedAppendStore>;

    beforeEach(async function () {
        mockStore = sandbox.spy(new MockedAppendStore(undefined, () => 10));
    });

    afterEach(async function () {
        sandbox.restore();
    });

    it('should not consolidate if there is only single entry in the store', async () => {
        const target = new Blocks(mockStore);
        const blockTypeFactory = new Map([[100, TestBlock.from]]);
        const payload = new TestBlock(Buffer.from("B1"), Buffer.from("H1"), mockStore);
        const bytesAppended = target.append(payload);
        assert.strictEqual(mockStore.store.length, bytesAppended);
        assert.strictEqual(target.consolidate(undefined, blockTypeFactory), false);
    });

    it('should be able to consolidate 3 blocks into one with correct skip entry.', async () => {
        const target = new Blocks(mockStore);
        const blockTypeFactory = new Map([[100, TestBlock.from]]);
        const payloads = [new TestBlock(Buffer.from("B1"), Buffer.from("H1"), mockStore), new TestBlock(Buffer.from("B2"), Buffer.from("H2"), mockStore), new TestBlock(Buffer.from("B3"), Buffer.from("H3"), mockStore)];

        const bytesAppended = payloads.reduce((acc, p) => acc + target.append(p), 0);
        assert.strictEqual(mockStore.store.length, bytesAppended);

        let cursor = target.iterate();
        let previousRemainingBytes = bytesAppended;
        let counter = payloads.length;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            const remainingBytes = result.value[1];
            const expected = payloads.pop();
            assert.equal(actual instanceof Block, true);
            assert.equal(actual instanceof TestBlock, false);
            assert.equal(remainingBytes < previousRemainingBytes, true);
            previousRemainingBytes = remainingBytes;
            assert.notEqual(expected, undefined);
            assert.notEqual(actual, undefined);
            assert.equal(actual.type, expected?.type);
            assert.deepStrictEqual(actual.body(), expected?.body());
            assert.deepStrictEqual(actual.header(), expected?.header());
            assert.deepStrictEqual(actual.bodyLength, expected?.bodyLength);
            assert.deepStrictEqual(actual.headerLength, expected?.headerLength);
            assert.deepStrictEqual(actual.store?.id, expected?.store?.id);
            result = cursor.next();
            counter--;
        }
        assert.equal(payloads.length, 0);
        assert.equal(target.cachedBlocks.size, 3);

        assert.strictEqual(target.consolidate(undefined, blockTypeFactory), true);
        assert.equal(target.cachedBlocks.size, 0);
        assert.equal(bytesAppended < mockStore.store.length, true);

        previousRemainingBytes = mockStore.store.length;
        cursor = target.iterate();
        counter = 0;
        result = cursor.next();
        const expectedHeader = Buffer.from("H3H2H1");
        const expectedBody = Buffer.from("B3B2B1");
        while (!result.done) {
            const actual = result.value[0];
            const remainingBytes = result.value[1];
            assert.equal(actual instanceof Block, true);
            assert.equal(actual instanceof TestBlock, false);
            assert.equal(remainingBytes < previousRemainingBytes, true);
            previousRemainingBytes = remainingBytes;
            assert.deepStrictEqual(actual.header(), expectedHeader);
            assert.deepStrictEqual(actual.body(), expectedBody);
            assert.equal(actual.blockPosition > 0, true);
            assert.equal(actual.headerLength, expectedHeader.length);
            assert.equal(actual.bodyLength, expectedBody.length);
            assert.equal(actual.type, 100);
            assert.equal(actual.store, mockStore);
            counter++;
            result = cursor.next();
        }
        assert.equal(counter, 1);
    });
});


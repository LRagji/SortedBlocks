import * as assert from 'node:assert';
import { Block, Blocks, CachePolicy, IAppendStore, LocalCache } from '../../source/index';
import { MockedAppendStore } from '../utilities/mock-store';
import { TestBlock } from '../utilities/test-block';
import sinon, { SinonSpiedInstance } from 'sinon';


describe(`Blocks iterate specs`, () => {
    const sandbox = sinon.createSandbox();
    let mockStore: SinonSpiedInstance<MockedAppendStore>;

    beforeEach(async function () {
        mockStore = sandbox.spy(new MockedAppendStore(undefined, () => 10));
    });

    afterEach(async function () {
        sandbox.restore();
    });

    it('should be able to read back appended blocks of fixed size.', async () => {
        const target = new Blocks(mockStore);
        const body = Buffer.from("Body"), header = Buffer.from("Header");
        const payloads = [new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore)];
        const bytesPerBlock = 28;
        const preambleByteLength = 18;

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        const cursor = target.iterate();
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
            assert.deepStrictEqual(actual.blockPosition, (bytesPerBlock * counter) - (preambleByteLength + 1));
            assert.deepStrictEqual(actual.store?.id, expected?.store?.id);
            result = cursor.next();
            counter--;
        }
        assert.equal(payloads.length, 0);
    });

    it('should be able to read back appended blocks of arbitary size.', async () => {
        const target = new Blocks(mockStore);
        const payloads = Array.from({ length: 200 }, (_, index) => new TestBlock(Buffer.from(`Body${index}`), Buffer.from(`Headersjjkakjdskjdk${index}`), mockStore));

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        const cursor = target.iterate();
        let previousRemainingBytes = bytesAppended;
        let counter = payloads.length;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            const remainingBytes = result.value[1];
            const expected = payloads.pop();
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
    });

    it('should be able to read null if underlying buffer input is empty.', async () => {
        const target = new Blocks(mockStore);
        const cursor = target.iterate();
        let counter = 0;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            assert.notEqual(actual, undefined);
            result = cursor.next();
            counter++;
        }
        assert.equal(counter, 0);
    });

    it('should be able to read back appended blocks even when buffer has garbage containing SOB.', async () => {
        const target = new Blocks(mockStore);
        const body = Buffer.from("Body"), header = Buffer.from("Header");
        const payloads = [new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore)];

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        mockStore.store = Buffer.concat([Buffer.from("I am #!pre Garbage#!"), mockStore.store, Buffer.from("I am #!post Garbage#!")]);

        const cursor = target.iterate();
        let previousRemainingBytes = bytesAppended;
        let counter = payloads.length;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            const remainingBytes = result.value[1];
            const expected = payloads.pop();
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
    });

    it('should cache already read blocks with default cache policy', async () => {
        const target = new Blocks(mockStore);
        const body = Buffer.from("Body"), header = Buffer.from("Header");
        const payloads = [new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore)];
        const preambleByteLength = 18;

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        let actualBlocks = new Map<number, Block>();
        const cursor = target.iterate();
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            actualBlocks.set(actual.blockPosition + preambleByteLength, actual);
            result = cursor.next();
        }
        assert.equal(actualBlocks.size, target.cacheContainer.length);
        assert.deepStrictEqual(Array.from(actualBlocks.keys(), (v, _) => `${mockStore.id}-${v}`), Array.from((target.cacheContainer as LocalCache).cache.keys()));
        assert.deepStrictEqual(Array.from(actualBlocks.values()), Array.from((target.cacheContainer as LocalCache).cache.values()));
    });

    it('should not cache already read blocks with none cache policy', async () => {
        const target = new Blocks(mockStore, CachePolicy.None);
        const body = Buffer.from("Body"), header = Buffer.from("Header");
        const payloads = [new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore), new TestBlock(body, header, mockStore)];
        const preambleByteLength = 18;

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        let actualBlocks = new Map<number, Block>();
        const cursor = target.iterate();
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            actualBlocks.set(actual.blockPosition + preambleByteLength, actual);
            result = cursor.next();
        }
        assert.equal(actualBlocks.size, payloads.length);
        assert.equal(target.cacheContainer.length, 0);
    });

    it('should be able to factory map for initializing the user defined block types.', async () => {
        const target = new Blocks(mockStore);
        const payloads = Array.from({ length: 200 }, (_, index) => new TestBlock(Buffer.from(`Body${index}`), Buffer.from(`Headersjjkakjdskjdk${index}`), mockStore));
        const blockTypeFactory = (b: Block) => b.type === TestBlock.type ? TestBlock.from(b.store as IAppendStore, b.type, b.blockPosition, b.headerLength, b.bodyLength) : b;

        let bytesAppended = 0;
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);

        const cursor = target.iterate(blockTypeFactory);
        let previousRemainingBytes = bytesAppended;
        let counter = payloads.length;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            assert.equal(actual instanceof TestBlock, true);
            assert.equal(actual instanceof Block, true);
            const remainingBytes = result.value[1];
            const expected = payloads.pop();
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
    });

    it('should be able to read back appended blocks of fixed size post multiple reads and appends.', async () => {
        const target = new Blocks(mockStore);
        const payloads = Array.from({ length: 3 }, (_, index) => new TestBlock(Buffer.from(`Body${index}`), Buffer.from(`Headersjjkakjdskjdk${index}`), mockStore));;

        let bytesAppended = await target.append(payloads[0]);
        assert.strictEqual(mockStore.store.length, bytesAppended);

        let cursor = target.iterate();
        let previousRemainingBytes = bytesAppended;
        let result = cursor.next();
        while (!result.done) {
            const actual = result.value[0];
            const remainingBytes = result.value[1];
            const expected = payloads[0];
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
        }
        //assert.equal(payloads.length, 2);
        const b = payloads.shift();
        for (let index = 0; index < payloads.length; index++) {
            const p = payloads[index];
            bytesAppended += await target.append(p);
        }
        assert.strictEqual(mockStore.store.length, bytesAppended);
        payloads.unshift(b as TestBlock);

        cursor = target.iterate();
        previousRemainingBytes = bytesAppended;
        result = cursor.next();
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
        }

        assert.equal(payloads.length, 0);
    });
});


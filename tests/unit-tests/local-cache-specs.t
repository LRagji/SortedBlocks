import * as assert from 'node:assert';
import { LocalCache } from '../../source/cache-proxy';
import { MockedAppendStore } from '../utilities/mock-store';
import sinon, { SinonSpiedInstance } from 'sinon';

let mockStore: SinonSpiedInstance<MockedAppendStore>;

describe(`LocalCache specs`, () => {
    const sandbox = sinon.createSandbox();

    beforeEach(async function () {
        mockStore = sandbox.spy(new MockedAppendStore(undefined, () => 10));
    });

    afterEach(async function () {
        sandbox.restore();
    });

    it('local-cache should return from cache when same offet read is asked for.', async () => {
        const target = new LocalCache();
        const expected = Buffer.from("0123456789");
        mockStore.append(expected);
        assert.strictEqual(mockStore.store.length, expected.length);

        let actual = target.reverseRead(9, mockStore, 1);
        assert.deepStrictEqual(actual, expected);
        assert.strictEqual(mockStore.readCallback.calledOnce, true);

        actual = target.reverseRead(9, mockStore, 1);
        assert.deepStrictEqual(actual, expected);
        assert.strictEqual(mockStore.readCallback.calledOnce, true);
    })

    it('local-cache should return from cache when ofset is between previous cached offset.', async () => {
        const target = new LocalCache();
        const expected = Buffer.from("0123456789");
        mockStore.append(expected);
        assert.strictEqual(mockStore.store.length, expected.length);

        let actual = target.reverseRead(9, mockStore, 1);
        assert.deepStrictEqual(actual, expected);
        assert.strictEqual(mockStore.readCallback.calledOnce, true);

        actual = target.reverseRead(5, mockStore, 1);
        assert.deepStrictEqual(actual, expected.subarray(0, 6));
        assert.strictEqual(mockStore.readCallback.calledOnce, true);
    })

    it('local-cache should not return zero bytes from cache.', async () => {
        const target = new LocalCache();
        const expected = Buffer.from("0123456789");
        mockStore.append(expected);
        assert.strictEqual(mockStore.store.length, expected.length);

        let actual = target.reverseRead(9, mockStore, 1);
        assert.deepStrictEqual(actual, expected);
        assert.strictEqual(mockStore.readCallback.calledOnce, true);

        actual = target.reverseRead(0, mockStore, 1);
        assert.deepStrictEqual(actual, expected.subarray(0, 1));
        assert.strictEqual(mockStore.readCallback.calledOnce, true);
    })
});
import * as assert from 'node:assert';
import { SOB, Blocks, MaxUint32 } from '../../source/blocks';
import { MockedAppendStore } from '../utilities/mock-store';
import { TestBlock } from '../utilities/test-block';
import sinon, { SinonSpiedInstance } from 'sinon';
import crc16 from 'crc/calculators/crc16';


describe(`Blocks append specs`, () => {
    const sandbox = sinon.createSandbox();
    let mockStore: SinonSpiedInstance<MockedAppendStore>;

    beforeEach(async function () {
        mockStore = sandbox.spy(new MockedAppendStore(undefined, () => 10));
    });

    afterEach(async function () {
        sandbox.restore();
    });

    it('should be assembling the packet in expected format.', async () => {
        const target = new Blocks(mockStore);
        const body = Buffer.from("Body"), header = Buffer.from("Header");
        const payload = new TestBlock(body, header);
        const bytesAppended = target.append(payload);

        const preamble = Buffer.alloc(18);
        preamble.writeUInt32BE(header.length);
        preamble.writeUInt32BE(body.length, 4);
        preamble.writeUInt32BE(payload.type, 8);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 12);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 14);
        preamble.writeUint8(SOB[0], 16);
        preamble.writeUint8(SOB[1], 17);
        const expectedBuffer = Buffer.concat([body, header, preamble]);

        assert.strictEqual(mockStore.store.length, bytesAppended);
        assert.strictEqual(mockStore.store.length, expectedBuffer.length);
        assert.strictEqual(mockStore.append.calledOnceWith(expectedBuffer), true);
    });

    it('should append for zero length header and body', async () => {
        const target = new Blocks(mockStore);
        const body = Buffer.alloc(0), header = Buffer.alloc(0);
        const payload = new TestBlock(body, header);
        const bytesAppended = target.append(payload);

        const preamble = Buffer.alloc(18);
        preamble.writeUInt32BE(header.length);
        preamble.writeUInt32BE(body.length, 4);
        preamble.writeUInt32BE(payload.type, 8);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 12);
        preamble.writeUInt16BE(crc16(preamble.subarray(0, 12)), 14);
        preamble.writeUint8(SOB[0], 16);
        preamble.writeUint8(SOB[1], 17);
        const expectedBuffer = Buffer.concat([body, header, preamble]);

        assert.strictEqual(mockStore.store.length, bytesAppended);
        assert.strictEqual(mockStore.store.length, expectedBuffer.length);
        assert.strictEqual(mockStore.append.calledOnceWith(expectedBuffer), true);
    });

    it('should not allow body greater than 4294967295', async () => {
        const target = new Blocks(mockStore);
        let body = Buffer.alloc(0), header = Buffer.alloc(0);
        sinon.stub(body, "length").value(MaxUint32 + 1);
        const payload = new TestBlock(body, header);
        assert.throws(() => target.append(payload), new Error(`Block body size cannot be more than ${MaxUint32}.`));
    })

    it('should not allow header greater than 4294967295', async () => {
        const target = new Blocks(mockStore);
        let body = Buffer.alloc(0), header = Buffer.alloc(0);
        sinon.stub(header, "length").value(MaxUint32 + 1);
        const payload = new TestBlock(body, header);
        assert.throws(() => target.append(payload), new Error(`Block header size cannot be more than ${MaxUint32}.`));
    })

    it('should not allow appends for system blocks 0 to 99', async () => {
        const target = new Blocks(mockStore);
        let body = Buffer.alloc(0), header = Buffer.alloc(0);
        const payload = new TestBlock(body, header);
        sinon.stub(payload, "type").value(0);
        assert.throws(() => target.append(payload), new Error(`Block type must be between 100 and ${MaxUint32}.`));
        sinon.stub(payload, "type").value(99);
        assert.throws(() => target.append(payload), new Error(`Block type must be between 100 and ${MaxUint32}.`));
    })
});


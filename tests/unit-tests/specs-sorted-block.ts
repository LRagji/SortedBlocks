import * as assert from 'node:assert';
import { append } from '../../source/index'

describe(`sorted-section write specs`, () => {
    const level1 = BigInt(1);
    const leve2_1 = BigInt(200);
    const leve2_2 = BigInt(300);
    const leve2_3 = BigInt(400);
    const leve3_1_2_1 = BigInt(5);
    const leve3_2_2_1 = BigInt(6);
    const leve3_3_2_2 = BigInt(7);
    const leve3_4_2_2 = BigInt(8);


    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should throw an exception for empty payload', async () => {
        const payload = new Map<BigUint64Array, ArrayBuffer>();
        assert.throws(() => append(payload), new Error(`Parameter "payload" cannot be empty.`));
    })

    it('should throw an exception for inconsistent depths of hierarchy', async () => {
        const payload = new Map<BigUint64Array, ArrayBuffer>();
        payload.set(BigUint64Array.from([level1]), Buffer.alloc(0))
        payload.set(BigUint64Array.from([level1, leve2_1, leve3_1_2_1]), Buffer.alloc(0));
        assert.throws(() => append(payload), new Error(`Parameter "payload" cannot have inconsistent hierarchy depth, expected depth ${Array.from(payload.keys())[0].length}.`));
    })

    it('should throw an exception for less than one parent is passed', async () => {
        const payload = new Map<BigUint64Array, ArrayBuffer>();
        payload.set(BigUint64Array.from([]), Buffer.alloc(0))
        payload.set(BigUint64Array.from([]), Buffer.alloc(0));
        assert.throws(() => append(payload), new Error(`Parameter "payload" cannot have a hierarchy depth less than 1.`));
    })

    it('should calculate correct bytes and buffers for payload with more than 1 level', async () => {
        const payload = new Map<BigUint64Array, ArrayBuffer>();
        payload.set(BigUint64Array.from([level1, leve2_1, BigInt(30)]), Buffer.alloc(1))
        payload.set(BigUint64Array.from([level1, leve2_2, BigInt(3)]), Buffer.alloc(10));
        payload.set(BigUint64Array.from([level1, leve2_3, BigInt(600)]), Buffer.alloc(1))
        const result = append(payload);
        assert.deepStrictEqual(result.metaBytes, 28 + 12 + 28 + (12 * 3) + 8 + (16 * 3) + 16);
        assert.deepStrictEqual(result.contentBytes, 12);
    })
})

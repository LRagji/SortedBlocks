import * as assert from 'assert';
import crypto from "crypto";
import { IKey, SortedBlocks, SortedVersions } from '../../source/index';
import { MockedStore } from '../utilities/mock-store';
const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));


describe(`sorted-blocks write specs v1`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should be able to generate sortedblocks for version 1', async () => {
        const mockStore = new MockedStore();
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const version = 1;
        const target = new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver, version);
        const blockId = "ID1";
        const records = [["Hello", "World"]];
        const charPurgedCount = target.append(blockId, records);
        assert.deepStrictEqual(charPurgedCount, mockStore.store.length);
        const versionRegistry = new SortedVersions();
        const currentVersion = versionRegistry.get(version);
        const key = indexResolver(records[0]);
        const data = `R,${records[0].join(",")}\n`;
        const dataHash = hashResolver(data);
        const index = `I,${key},0,${data.length}\n`;
        const indexHash = hashResolver(index);
        const preHeader = `H,${blockId},${currentVersion.number},${index.length},${data.length},${indexHash},${dataHash},${indexHash},${dataHash}`;
        const headerHash = hashResolver(preHeader);
        const postHeader = `,${headerHash},${preHeader.length},${headerHash}`;
        const header = preHeader + postHeader + "\n";
        assert.deepStrictEqual(mockStore.read(0, data.length), data);
        assert.deepStrictEqual(mockStore.read(data.length, index.length), index);
        const start = data.length + index.length;
        assert.deepStrictEqual(mockStore.read(start, mockStore.store.length - start), header);
        assert.deepStrictEqual(mockStore.store, `${data}${index}${header}`);
    })

    it('should be able to sort and group multiple records for version 1', async () => {
        const mockStore = new MockedStore();
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const version = 1;
        const target = new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver, version);
        const blockId = "ID1";
        const records = [["Hello", "1"], ["Hello", "2"], ["Hello", "3"], ["H3llo", "World"]];
        const charPurgedCount = target.append(blockId, records);
        assert.deepStrictEqual(charPurgedCount, mockStore.store.length);
        const versionRegistry = new SortedVersions();
        const currentVersion = versionRegistry.get(version);
        const sortedRecords = records.sort(sortResolver);
        const sortedSerializedRecords = sortedRecords.map(r => `R,${r.join(",")}\n`);
        const data = sortedSerializedRecords.join(``);
        const dataHash = hashResolver(data);
        const keys = sortedRecords.map(indexResolver);
        const keyLengths = keys.reduce((acc, e, idx) => { acc.set(e, ((acc.get(e) || 0) + sortedSerializedRecords[idx].length)); return acc }, new Map<IKey, number>());
        let previousEnd = 0;
        let index = "";
        keyLengths.forEach((length, key) => {
            index += `I,${key},${previousEnd},${length}\n`;
            previousEnd = length;
        });
        const indexHash = hashResolver(index);
        const preHeader = `H,${blockId},${currentVersion.number},${index.length},${data.length},${indexHash},${dataHash},${indexHash},${dataHash}`;
        const headerHash = hashResolver(preHeader);
        const postHeader = `,${headerHash},${preHeader.length},${headerHash}`;
        const header = preHeader + postHeader + "\n";
        assert.deepStrictEqual(mockStore.read(0, data.length), data);
        assert.deepStrictEqual(mockStore.read(data.length, index.length), index);
        const start = data.length + index.length;
        assert.deepStrictEqual(mockStore.read(start, mockStore.store.length - start), header);
        assert.deepStrictEqual(mockStore.store, `${data}${index}${header}`);
    })

    it('should throw and error for unsupported version', async () => {
        const mockStore = new MockedStore();
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const version = 0;
        assert.throws(() => new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver, version), new Error("Unknown version specified 0"));
    })
});
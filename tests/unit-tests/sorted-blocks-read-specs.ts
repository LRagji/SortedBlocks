import * as assert from 'assert';
import crypto from "crypto";
import { IKey, SortedBlocks, SortedHeader, SortedVersions } from '../../source/index';
import { MockedStore } from '../utilities/mock-store';
const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));


describe(`sorted-blocks read specs v1`, () => {

    beforeEach(async function () {

    });

    afterEach(async function () {

    });

    it('should be able to read generated sortedblocks for version 1', async () => {
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
        const header = target.search();
        assert.notEqual(header, undefined);
        assert.deepStrictEqual(header?.next(), undefined);
        assert.deepStrictEqual(header?.BlockId, blockId);
        assert.deepStrictEqual(header?.version.number, version);

        const index = header?.index();
        assert.notEqual(index, undefined);
        assert.deepStrictEqual(index.version.number, version);
        assert.deepStrictEqual(index.entries.size, records.length);

        const IndexedRecord = index?.entries.get(records[0][0]);
        assert.notEqual(IndexedRecord, undefined);
        if (IndexedRecord != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord), records)

    })

    it('should be able to read multiple records for a single index key', async () => {
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
        const header = target.search();
        assert.notEqual(header, undefined);
        assert.deepStrictEqual(header?.next(), undefined);
        assert.deepStrictEqual(header?.BlockId, blockId);
        assert.deepStrictEqual(header?.version.number, version);

        const index = header?.index();
        assert.notEqual(index, undefined);
        assert.deepStrictEqual(index.version.number, version);
        assert.deepStrictEqual(index.entries.size, 2);

        let IndexedRecord = index?.entries.get("Hello");
        assert.notEqual(IndexedRecord, undefined);
        if (IndexedRecord != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord), [["Hello", "1"], ["Hello", "2"], ["Hello", "3"]]);

        IndexedRecord = index?.entries.get("H3llo");
        assert.notEqual(IndexedRecord, undefined);
        if (IndexedRecord != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord), [["H3llo", "World"]])
    })

    it('should default to version 1', async () => {
        const mockStore = new MockedStore();
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const target = new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver);
        assert.deepStrictEqual(target.version.number, 1);
    })

    it('should be able to read multiple sortedblocks', async () => {
        const mockStore = new MockedStore();
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const version = 1;
        const target = new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver, version);
        const blockId1 = "ID1";
        const records = [["Hello", "World"]];
        let charPurgedCount = target.append(blockId1, records);
        const blockId2 = "ID2";
        charPurgedCount += target.append(blockId2, records);
        assert.deepStrictEqual(charPurgedCount, mockStore.store.length);

        const header2 = target.search();
        assert.notEqual(header2, undefined);
        assert.deepStrictEqual(header2?.BlockId, blockId2);
        assert.deepStrictEqual(header2?.version.number, version);

        const index = header2?.index();
        assert.notEqual(index, undefined);
        assert.deepStrictEqual(index.version.number, version);
        assert.deepStrictEqual(index.entries.size, records.length);

        const IndexedRecord = index?.entries.get(records[0][0]);
        assert.notEqual(IndexedRecord, undefined);
        if (IndexedRecord != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord), records)

        const header1 = header2?.next();
        assert.notEqual(header1, undefined);
        assert.deepStrictEqual(header1?.next(), undefined);
        assert.deepStrictEqual(header1?.BlockId, blockId1);
        assert.deepStrictEqual(header1?.version.number, version);

        const index1 = header1?.index();
        assert.notEqual(index1, undefined);
        assert.deepStrictEqual(index1?.version.number, version);
        assert.deepStrictEqual(index1?.entries.size, records.length);

        const IndexedRecord1 = index?.entries.get(records[0][0]);
        assert.notEqual(IndexedRecord1, undefined);
        if (IndexedRecord1 != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord1), records)
    })

    it('should be able to read multiple sortedblocks with smaller buffer size but greater than one header segment length ', async () => {
        const mockStore = new MockedStore();
        mockStore.OptimalBufferSize = 300;
        const sortResolver = (lhs: string[], rhs: string[]) => lhs[0].charCodeAt(0) - rhs[0].charCodeAt(0);
        const hashResolver = (data: string) => crypto.createHash('md5').update(data).digest('hex');
        const indexResolver = (record: string[]) => (record[0] as IKey);
        const version = 1;
        const target = new SortedBlocks(mockStore, sortResolver, hashResolver, indexResolver, version);
        const blockId1 = "ID1";
        const records = [["Hello", "World"]];
        let charPurgedCount = target.append(blockId1, records);
        const blockId2 = "ID2";
        charPurgedCount += target.append(blockId2, records);
        assert.deepStrictEqual(charPurgedCount, mockStore.store.length);

        const header2 = target.search();
        assert.notEqual(header2, undefined);
        assert.deepStrictEqual(header2?.BlockId, blockId2);
        assert.deepStrictEqual(header2?.version.number, version);

        const index = header2?.index();
        assert.notEqual(index, undefined);
        assert.deepStrictEqual(index.version.number, version);
        assert.deepStrictEqual(index.entries.size, records.length);

        const IndexedRecord = index?.entries.get(records[0][0]);
        assert.notEqual(IndexedRecord, undefined);
        if (IndexedRecord != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord), records)

        const header1 = header2?.next();
        assert.notEqual(header1, undefined);
        assert.deepStrictEqual(header1?.next(), undefined);
        assert.deepStrictEqual(header1?.BlockId, blockId1);
        assert.deepStrictEqual(header1?.version.number, version);

        const index1 = header1?.index();
        assert.notEqual(index1, undefined);
        assert.deepStrictEqual(index1?.version.number, version);
        assert.deepStrictEqual(index1?.entries.size, records.length);

        const IndexedRecord1 = index?.entries.get(records[0][0]);
        assert.notEqual(IndexedRecord1, undefined);
        if (IndexedRecord1 != undefined) assert.deepStrictEqual(target.fetchAssociatedRecords(IndexedRecord1), records)
    })
});
export interface IStore {
    Id: string;
    append(data: string): void;
    read(position: number, utf8CharactersToRead: number): string;
    fetchCurrentSize(): number;
}

export interface IKey {
    key: string,
}

export interface IIndex extends IKey {
    position: number;
    length: number;
}

interface tempHolder {
    serializedDataBlock: string,
    serializedIndexBlock: string
}

interface versionOneInfo extends IVersionInfo {
    dataRecordType: string,
    indexRecordType: string,
    headerRecordType: string,
    recordSeperatorChar: string,
    fieldSeperatorChar: string,
    fieldsInHeader: number,
    fieldsInIndex: number,
    minFieldsInRecord: number,
    headerVersionFieldIndex: number,
    headerIndex1FieldIndex: number,
    headerIndex2FieldIndex: number,
    headerData1FieldIndex: number,
    headerData2FieldIndex: number,
    headerBlockIdFieldIndex: number,
    headerIndexLengthFieldIndex: number,
    headerDataLengthFieldIndex: number,
    headerPartialLengthFieldIndex: number,
    headerHash1FieldIndex: number,
    headerHash2FieldIndex: number,
    bufferSize: number,
}

export interface IVersionInfo {
    number: number;
}

export class SortedVersions {

    private versionInfo = new Map<number, IVersionInfo>([[1, {
        dataRecordType: "R",
        indexRecordType: "I",
        headerRecordType: "H",
        fieldsInHeader: 12,
        fieldsInIndex: 4,
        minFieldsInRecord: 2,
        recordSeperatorChar: "\n",
        fieldSeperatorChar: ",",
        headerVersionFieldIndex: 2,
        headerIndex1FieldIndex: 5,
        headerIndex2FieldIndex: 7,
        headerData1FieldIndex: 6,
        headerData2FieldIndex: 8,
        headerBlockIdFieldIndex: 1,
        headerIndexLengthFieldIndex: 3,
        headerDataLengthFieldIndex: 4,
        headerPartialLengthFieldIndex: 11,
        headerHash1FieldIndex: 10,
        headerHash2FieldIndex: 12,
        bufferSize: 4096
    } as versionOneInfo]]);

    public get(version: number): IVersionInfo {
        const info = this.versionInfo.get(version);
        if (info == undefined) {
            throw new Error(`Unknown version specified ${version}`)
        }
        return info;
    }
}

export class SortedBlocks {
    private versionRegistry = new SortedVersions();

    constructor(
        private store: IStore,
        private sortResolver: (lhs: string[], rhs: string[]) => number,
        private hashResolver: (serializedData: string) => string,
        private indexResolver: (record: string[]) => IKey,
        public readonly currentVersion: number = 1
    ) { }

    public append(blockId: string, records: string[][]): number {
        const versionInfo = this.versionRegistry.get(this.currentVersion);
        let sortedRecords = records.sort(this.sortResolver);
        const Index = new Map<string, IIndex>();
        const context = SortedIndex.serializeIndexBlock(sortedRecords, this.indexResolver, versionInfo);
        const serializedHeaderBlock = SortedHeader.serializeHeaderBlock(blockId, context, versionInfo, this.hashResolver);
        const serializedBlock = `${context.serializedDataBlock}${context.serializedIndexBlock}${serializedHeaderBlock}`;
        this.store.append(serializedBlock);
        return serializedBlock.length
    }

    public search(): SortedHeader | undefined {
        const versionInfo = this.versionRegistry.get(this.currentVersion);
        return SortedHeader.from(this.store, versionInfo, this.hashResolver);
    }
}

export class SortedIndex {

    public static from(serializedIndex: string, header: SortedHeader, dataBlockStartPosition: number): SortedIndex {
        switch (header.version.number) {
            case 1:
                const currentVersion = header.version as versionOneInfo;
                const finalIndex = new Map<string, IIndex>();
                if (header.IndexHash != undefined && header.hashResolver != undefined) {
                    const computedHash = header.hashResolver(serializedIndex);
                    if (computedHash != header.IndexHash) {
                        throw new Error(`Hash for index(${header.IndexHash}) doesnt match the computed(${computedHash}) one.`)
                    }
                }
                const skippedEntries = [];
                const index = serializedIndex.split(currentVersion.recordSeperatorChar);
                index.pop();//An Extra end line
                for (let pointer = 0; pointer < index.length; pointer++) {
                    const entry = index[pointer];
                    const values = entry.split(currentVersion.fieldSeperatorChar);
                    if (values.length === currentVersion.fieldsInIndex && values[0] === currentVersion.indexRecordType) {
                        const key = values[1];
                        const relativePosition = parseInt(values[2]);
                        const length = parseInt(values[3]);
                        const absolutePosition = dataBlockStartPosition + relativePosition;
                        finalIndex.set(key, { "position": absolutePosition, "length": length, "key": key });
                    }
                    else {
                        skippedEntries.push(`Skipped Index item(${entry}) at position(${pointer}) as it is invalid.`)
                    }
                }
                return new SortedIndex(finalIndex, skippedEntries, currentVersion, header.store, finalIndex.size);
                break;
            default:
                throw new Error(`Version ${header.version.number} is not supported for deserializing index.`);
                break;
        }
    }

    private constructor(private map: Map<string, IIndex>, public readonly skippedEntries: string[], public readonly version: IVersionInfo, private store: IStore, public readonly entriesCount: number) { }

    public get(key: IKey): IIndex | undefined {
        return this.map.get(key.key);
    }

    public records(key: IKey): string[][] {
        const indexRecord = this.get(key);
        if (indexRecord == undefined) {
            return [];
        }
        else {
            switch (this.version.number) {
                case 1:
                    const currentVersion = this.version as versionOneInfo;
                    const serializedData = this.store.read(indexRecord.position, indexRecord.length);
                    return serializedData.split(currentVersion.recordSeperatorChar)
                        .reduce((acc, data) => {
                            const fields = data.split(currentVersion.fieldSeperatorChar);
                            if (fields.length > currentVersion.minFieldsInRecord && fields[0] === currentVersion.dataRecordType) {
                                fields.shift();
                                acc.push(fields);
                            }
                            return acc;
                        }, new Array<string[]>());
                    break;
                default:
                    throw new Error(`Version ${this.version.number} is not supported for deserializing records.`);
                    break;
            }
        }
    }

    private static serializeRecord(record: string[], version: IVersionInfo) {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                return currentVersion.dataRecordType + record.join(currentVersion.fieldSeperatorChar) + currentVersion.recordSeperatorChar;
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for serializing index.`);
                break;
        }
    }

    private static serializeDataBlock(sortedRecords: string[][], indexMapping: Map<string, IIndex>, indexResolver: (record: string[]) => IKey, version: IVersionInfo): string {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                let serializedDataBlock = "";
                for (let recordIdx = 0; recordIdx < sortedRecords.length; recordIdx++) {
                    const record = sortedRecords[recordIdx];
                    const indexKey = indexResolver(record);
                    const serializedRecord = SortedIndex.serializeRecord(record, currentVersion);
                    const indexEntry = { "key": indexKey.key, "position": serializedDataBlock.length, "length": serializedRecord.length }
                    const existingIndexEntry = indexMapping.get(indexEntry.key);
                    if (existingIndexEntry != undefined) {
                        indexEntry.length += existingIndexEntry.length;
                        indexEntry.position = existingIndexEntry.position;
                    }
                    serializedDataBlock += serializedRecord;
                    indexMapping.set(indexEntry.key, indexEntry);
                }
                return serializedDataBlock;
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for serializing data.`);
                break;
        }
    }

    public static serializeIndexBlock(sortedRecords: string[][], indexResolver: (record: string[]) => IKey, version: IVersionInfo): tempHolder {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                const indexMapping = new Map<string, IIndex>();
                const serializedDataBlock = SortedIndex.serializeDataBlock(sortedRecords, indexMapping, indexResolver, currentVersion);
                return {
                    "serializedDataBlock": serializedDataBlock,
                    "serializedIndexBlock": Array.from(indexMapping.values())
                        .map((indexRecord) => {
                            return currentVersion.indexRecordType
                                + currentVersion.fieldSeperatorChar
                                + indexRecord.key
                                + currentVersion.fieldSeperatorChar
                                + indexRecord.position.toString()
                                + currentVersion.fieldSeperatorChar
                                + (indexRecord.length - 1).toString()
                                + currentVersion.recordSeperatorChar;
                        })
                        .join("")
                };
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for serializing index.`);
                break;
        }
    }
}

export class SortedHeader {
    public readonly BlockId: number
    public readonly IndexHash: string
    public readonly DataHash: string
    public readonly HeaderHash: string
    public readonly IndexLength: number
    public readonly DataLength: number

    public static from(store: IStore, version: IVersionInfo, hashResolver: ((serializedData: string) => string) | undefined, start: number = store.fetchCurrentSize()): SortedHeader | undefined {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                const magicStartSequence = `${currentVersion.headerRecordType}${currentVersion.fieldSeperatorChar}`;
                let fileCursor = start - currentVersion.bufferSize;
                do {
                    const currentRead = store.read(Math.max(fileCursor, 0), currentVersion.bufferSize)
                    fileCursor = (fileCursor + currentVersion.bufferSize) - currentRead.length;
                    let baseReadLength = currentRead.length;
                    const headerItems = currentRead.split(currentVersion.recordSeperatorChar);
                    if (fileCursor > 0) {
                        //Need to do this cause we are not yet sure of the first element if it is split cause of a endline or half read
                        const skippedLineLength = (headerItems.shift() || "").length;
                        fileCursor += skippedLineLength;
                        baseReadLength -= skippedLineLength;
                    }

                    const headerPointer = headerItems
                        .reverse()//Scan from behind
                        .reduce((acc, rawHeader, idx, arr) => {
                            if (acc.found === false) {
                                acc.found = rawHeader.startsWith(magicStartSequence)
                                    && rawHeader.split(currentVersion.fieldSeperatorChar).length === currentVersion.fieldsInHeader
                                    && currentVersion.number !== parseInt(rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerVersionFieldIndex], 10)
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerIndex1FieldIndex] !== rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerIndex2FieldIndex]
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerData1FieldIndex] !== rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerData2FieldIndex]
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash1FieldIndex] !== rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash2FieldIndex]
                                    && hashResolver != undefined ?
                                    rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash1FieldIndex] !== hashResolver(rawHeader.substring(0, parseInt(rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerPartialLengthFieldIndex], 10)))
                                    : true;
                                acc.absolutePosition -= (rawHeader.length + 1);

                                if (acc.found === true) {
                                    acc.rawValues = rawHeader.split(currentVersion.fieldSeperatorChar);
                                    arr.splice(1);//Equvalent to break;
                                    acc.length = rawHeader.length;
                                }
                            }
                            return acc;
                        }, { found: false, absolutePosition: (fileCursor + baseReadLength), rawValues: new Array<string>(), length: 0 });

                    if (headerPointer.found) {
                        return new SortedHeader(headerPointer.absolutePosition, headerPointer.length, currentVersion, store, headerPointer.rawValues, hashResolver);
                    }
                    else {
                        fileCursor -= currentVersion.bufferSize;
                    }
                }
                while (fileCursor > 0)
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for deserializing headers.`);
                break;
        }
    }

    private constructor(public readonly absolutePosition: number, public readonly length: number, public version: IVersionInfo, public store: IStore, rawValues: string[], public readonly hashResolver: ((serializedData: string) => string) | undefined) {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                this.BlockId = parseInt(rawValues[currentVersion.headerBlockIdFieldIndex], 10);
                this.IndexLength = parseInt(rawValues[currentVersion.headerIndexLengthFieldIndex], 10);
                this.DataLength = parseInt(rawValues[currentVersion.headerDataLengthFieldIndex], 10);
                this.IndexHash = rawValues[currentVersion.headerIndex1FieldIndex];
                this.DataHash = rawValues[currentVersion.headerData1FieldIndex];
                this.HeaderHash = rawValues[currentVersion.headerHash1FieldIndex];
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for deserializing headers.`);
                break;
        }
    }

    public next(): SortedHeader | undefined {
        const dataStartPosition = this.absolutePosition - (this.IndexLength + this.DataLength);
        return SortedHeader.from(this.store, this.version, this.hashResolver, dataStartPosition);
    }

    public index(): SortedIndex {
        const indexStartPosition = this.absolutePosition;
        const dataStartPosition = this.absolutePosition - (this.IndexLength + this.DataLength);
        const serializedIndex = this.store.read(indexStartPosition, this.IndexLength);
        return SortedIndex.from(serializedIndex, this, dataStartPosition);
    }

    public static serializeHeaderBlock(blockId: string, context: tempHolder, version: IVersionInfo, hashResolver: ((serializedData: string) => string) | undefined): string {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                const indexHash = hashResolver != undefined ? hashResolver(context.serializedIndexBlock) : "";
                const dataHash = hashResolver != undefined ? hashResolver(context.serializedDataBlock) : "";
                const header = currentVersion.headerRecordType
                    + currentVersion.fieldSeperatorChar
                    + blockId
                    + currentVersion.fieldSeperatorChar
                    + currentVersion.number.toString()
                    + currentVersion.fieldSeperatorChar
                    + context.serializedIndexBlock.length.toString()
                    + currentVersion.fieldSeperatorChar
                    + context.serializedDataBlock.length.toString()
                    + currentVersion.fieldSeperatorChar
                    + indexHash
                    + currentVersion.fieldSeperatorChar
                    + dataHash
                    + currentVersion.fieldSeperatorChar
                    + indexHash
                    + currentVersion.fieldSeperatorChar
                    + dataHash;
                const headerHash = hashResolver != undefined ? hashResolver(header) : "";
                return header
                    + currentVersion.fieldSeperatorChar
                    + headerHash
                    + currentVersion.fieldSeperatorChar
                    + header.length
                    + currentVersion.fieldSeperatorChar
                    + headerHash
                    + currentVersion.recordSeperatorChar;
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for serializing headers.`);
                break;
        }
    }
}
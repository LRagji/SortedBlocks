export interface IStore {
    Id: string;
    OptimalBufferSize: number | undefined
    append(data: string): void;
    read(position: number, utf8CharactersToRead: number): string;
    fetchCurrentSize(): number;
}

export interface IKey extends String { }

export interface IIndexEntry {
    key: IKey;
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
    headerIndexOffsetFieldIndex: number,
    headerDataOffsetFieldIndex: number,
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
        headerIndexOffsetFieldIndex: 3,
        headerDataOffsetFieldIndex: 4,
        headerPartialLengthFieldIndex: 10,
        headerHash1FieldIndex: 9,
        headerHash2FieldIndex: 11,
        bufferSize: 4096,
        number: 1
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
    public readonly version: IVersionInfo;

    constructor(
        public readonly store: IStore,
        private sortResolver: (lhs: string[], rhs: string[]) => number,
        private hashResolver: (serializedData: string) => string,
        private indexResolver: (record: string[]) => IKey,
        currentVersion: number = 1
    ) { this.version = this.versionRegistry.get(currentVersion); }

    public append(blockId: string, records: string[][]): number {
        let sortedRecords = records.sort(this.sortResolver);
        const context = SortedIndex.serializeIndexBlock(sortedRecords, this.indexResolver, this.version);
        const serializedHeaderBlock = SortedHeader.serializeHeaderBlock(blockId, context, this.version, this.hashResolver);
        const serializedBlock = `${context.serializedDataBlock}${context.serializedIndexBlock}${serializedHeaderBlock}`;
        this.store.append(serializedBlock);
        return serializedBlock.length
    }

    public search(): SortedHeader | undefined {
        return SortedHeader.from(this.store, this.version, this.hashResolver);
    }
}

export class SortedIndex {

    public static from(serializedIndex: string, header: SortedHeader, dataBlockStartPosition: number): SortedIndex {
        switch (header.version.number) {
            case 1:
                const currentVersion = header.version as versionOneInfo;
                if (header.IndexHash != undefined && header.hashResolver != undefined) {
                    const computedHash = header.hashResolver(serializedIndex);
                    if (computedHash != header.IndexHash) {
                        throw new Error(`Hash for index(${header.IndexHash}) doesnt match the computed(${computedHash}) one.`)
                    }
                }
                const finalIndex = serializedIndex.split(currentVersion.recordSeperatorChar)
                    .reduce((acc, entry, idx) => {
                        const values = entry.split(currentVersion.fieldSeperatorChar);
                        if (values.length === currentVersion.fieldsInIndex && values[0] === currentVersion.indexRecordType) {
                            const key = values[1];
                            const relativePosition = parseInt(values[2]);
                            const length = parseInt(values[3]);
                            const absolutePosition = dataBlockStartPosition + relativePosition;
                            acc.set(key, { "position": absolutePosition, "length": length, "key": key });
                        }
                        return acc;
                    }, new Map<IKey, IIndexEntry>())
                return new SortedIndex(finalIndex, currentVersion, header.store);
                break;
            default:
                throw new Error(`Version ${header.version.number} is not supported for deserializing index.`);
                break;
        }
    }

    private constructor(public entries: Map<IKey, IIndexEntry>, public readonly version: IVersionInfo, private store: IStore) { }

    public fetchAssociatedRecords(indexedEntry: IIndexEntry): string[][] | undefined {
        switch (this.version.number) {
            case 1:
                const currentVersion = this.version as versionOneInfo;
                const serializedData = this.store.read(indexedEntry.position, indexedEntry.length);
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

    private static serializeRecord(record: string[], version: IVersionInfo) {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                return currentVersion.dataRecordType + currentVersion.fieldSeperatorChar + record.join(currentVersion.fieldSeperatorChar) + currentVersion.recordSeperatorChar;
                break;
            default:
                throw new Error(`Version ${version.number} is not supported for serializing index.`);
                break;
        }
    }

    private static serializeDataBlock(sortedRecords: string[][], indexMapping: Map<IKey, IIndexEntry>, indexResolver: (record: string[]) => IKey, version: IVersionInfo): string {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                let serializedDataBlock = "";
                for (let recordIdx = 0; recordIdx < sortedRecords.length; recordIdx++) {
                    const record = sortedRecords[recordIdx];
                    const indexKey = indexResolver(record);
                    const serializedRecord = SortedIndex.serializeRecord(record, currentVersion);
                    const indexEntry = { "key": indexKey, "position": serializedDataBlock.length, "length": serializedRecord.length }
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
                const indexMapping = new Map<IKey, IIndexEntry>();
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
                                + indexRecord.length.toString()
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
    public readonly BlockId: string
    public readonly IndexHash: string
    public readonly DataHash: string
    public readonly HeaderHash: string
    public readonly IndexOffset: number
    public readonly DataOffset: number

    public static from(store: IStore, version: IVersionInfo, hashResolver: ((serializedData: string) => string) | undefined, start: number = store.fetchCurrentSize()): SortedHeader | undefined {
        switch (version.number) {
            case 1:
                const currentVersion = version as versionOneInfo;
                const magicStartSequence = `${currentVersion.headerRecordType}${currentVersion.fieldSeperatorChar}`;
                const bufferSize = Math.min((store.OptimalBufferSize == undefined ? currentVersion.bufferSize : store.OptimalBufferSize), start);
                let fileCursor = start - bufferSize;
                do {
                    const currentRead = store.read(Math.max(fileCursor, 0), bufferSize)
                    fileCursor = (fileCursor + bufferSize) - currentRead.length;
                    if (fileCursor < 0) return undefined; //This occurs when we read EOF(End of file)
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
                                    && currentVersion.number === parseInt(rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerVersionFieldIndex], 10)
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerIndex1FieldIndex] === rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerIndex2FieldIndex]
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerData1FieldIndex] === rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerData2FieldIndex]
                                    && rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash1FieldIndex] === rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash2FieldIndex]
                                    && (hashResolver != undefined ?
                                        rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerHash1FieldIndex] === hashResolver(rawHeader.substring(0, parseInt(rawHeader.split(currentVersion.fieldSeperatorChar)[currentVersion.headerPartialLengthFieldIndex], 10)))
                                        : true);
                                acc.absolutePosition -= (rawHeader.length + 1);

                                if (acc.found === true) {
                                    acc.rawValues = rawHeader.split(currentVersion.fieldSeperatorChar);
                                    arr.splice(1);//Equvalent to break;
                                    acc.length = rawHeader.length;
                                    acc.absolutePosition++;
                                }
                            }
                            return acc;
                        }, { found: false, absolutePosition: (fileCursor + baseReadLength), rawValues: new Array<string>(), length: 0 });

                    if (headerPointer.found) {
                        return new SortedHeader(headerPointer.absolutePosition, headerPointer.length, currentVersion, store, headerPointer.rawValues, hashResolver);
                    }
                    else {
                        fileCursor -= bufferSize;
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
                this.BlockId = rawValues[currentVersion.headerBlockIdFieldIndex];
                this.IndexOffset = parseInt(rawValues[currentVersion.headerIndexOffsetFieldIndex], 10);
                this.DataOffset = parseInt(rawValues[currentVersion.headerDataOffsetFieldIndex], 10);
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
        const dataStartPosition = this.absolutePosition - (this.IndexOffset + this.DataOffset);
        return SortedHeader.from(this.store, this.version, this.hashResolver, dataStartPosition);
    }

    public index(): SortedIndex {
        const indexStartPosition = this.absolutePosition - this.IndexOffset;
        const dataStartPosition = this.absolutePosition - (this.IndexOffset + this.DataOffset);
        const serializedIndex = this.store.read(indexStartPosition, this.IndexOffset);
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
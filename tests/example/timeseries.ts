const tags = 1000000n
const payload = new Map<bigint, Buffer>();
for (let tagIdx = 0n; tagIdx < tags; tagIdx++) {
    const sample = Buffer.alloc(8 + 8 + 16);//Time,Value,Quality
    sample.writeBigUInt64BE(tagIdx);//Time
    sample.writeDoubleBE(12 + 0.5, 8);//Value
    sample.writeBigUInt64BE(tagIdx, 16);//Quality 1
    sample.writeBigUInt64BE(tagIdx), 24;//Quality 2

}
// According to the official Apache Arrow IPC specification, 
// a serialized ['RecordBatch'] is a single contiguous byte stream formatted as:
// ┌--------------------------------------------------------------------------------┐
// │  0xFFFFFFFF  │ Metadata Size │ Metadata FlatBuffer  │ Padding  │  Body Bytes   │
// │  (4 bytes)   │   (4 bytes)   │ (RecordBatch Header) │ (8-byte) │ (Raw Buffers) │
// └--------------------------------------------------------------------------------┘
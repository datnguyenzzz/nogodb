use std::{
    io::{Read, Write},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, VectorIter, root};

use crate::arrow::{
    ArrayRef, BooleanBuffer, Buffer, DataType, Field, NullBuffer, RecordBatch, Schema, SchemaRef,
    array::{BooleanArray, PrimitiveArray, StringArray},
    ipc::{
        FbMessage, FbMessageBuilder, FbMetaNode, FbMetaNodeBuilder, FbMetaSegment,
        FbMetaSegmentBuilder, FbRecordBatch, FbRecordBatchBuilder,
        schema::{fb_to_schema, schema_to_fb_offset},
    },
};

const ALIGNMENT: usize = 8 - 1;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const PADDING: [u8; 8] = [0u8; 8];

/// Arrow Writer
/// Writes Arrow [`RecordBatch`] to bytes using the [IPC Streaming Format].
/// https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
pub struct Writer<W: Write> {
    writer: W,
    schema_written: bool,
    compress: bool,
}

impl<W: Write> Writer<W> {
    #[inline]
    fn align(x: usize) -> usize {
        (x + ALIGNMENT) & !ALIGNMENT
    }

    pub fn new(writer: W) -> Self {
        Self {
            writer,
            schema_written: false,
            compress: true,
        }
    }

    /// Write a record batch to the stream, propagating writing errors up safely
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.schema_written {
            self.write_schema(batch.schema())?;
            self.schema_written = true;
        }

        self.write_record_batch(batch)?;
        Ok(())
    }

    /// Serializes and compresses a `RecordBatch` into a standard, zero-deserialization Arrow IPC packet
    fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut body_bytes: Vec<u8> = Vec::new();
        // represents (column length, null count) of column
        let mut nodes: Vec<(i64, i64)> = Vec::new();
        // represents (offset, length) of a continous decoded block
        let mut segments: Vec<(i64, i64)> = Vec::new();

        let mut compress_and_encode = |segments: &mut Vec<(i64, i64)>, opt: Option<Buffer>| {
            let start = body_bytes.len();

            if let Some(buf) = opt {
                if self.compress {
                    let uncompressed_len = buf.len();
                    let compressed = zstd::stream::encode_all(buf.as_slice(), 0)
                        .expect("Failed to compress buffer with Zstd");

                    if compressed.len() < uncompressed_len {
                        // Prepend uncompressed length (i64, little-endian) per spec
                        body_bytes.extend_from_slice(&(uncompressed_len as i64).to_le_bytes());
                        body_bytes.extend_from_slice(&compressed);
                    } else {
                        // Compression was not effective; fall back to uncompressed and write -1 as prefix
                        // this is Arrow IPC 's specification when writing uncompressed data
                        body_bytes.extend_from_slice(&(-1i64).to_le_bytes());
                        body_bytes.extend_from_slice(buf.as_slice());
                    }
                } else {
                    body_bytes.extend_from_slice(buf.as_slice());
                }

                let len = body_bytes.len() - start;
                segments.push((start as i64, len as i64));

                // Padding each buffer chunk to an 8-byte boundary per Arrow spec
                let aligned_len = Self::align(body_bytes.len());
                let padding = aligned_len - body_bytes.len();
                body_bytes.extend_from_slice(&PADDING[..padding]);
            } else {
                // Buffer is None (e.g. nullable column with 0 nulls, validity mask omitted)
                segments.push((start as i64, 0));
            }
        };

        // Gather all nodes (logical properties) and segments (physical offsets)
        for col_ref in batch.columns() {
            nodes.push((
                col_ref.len() as i64,
                col_ref.nulls().map(|n| n.null_count()).unwrap_or_default() as i64,
            ));

            compress_and_encode(
                &mut segments,
                col_ref.nulls().map(|n| n.buffer.buffer.clone()),
            );

            for buf in col_ref.buffers() {
                compress_and_encode(&mut segments, Some(buf));
            }
        }

        self.assemble_and_write(batch.num_rows() as i32, body_bytes, nodes, segments)?;
        Ok(())
    }

    /// Assemble the IPC Metadata and body message and write to the downstream
    /// The body_bytes must be padding to 8-byte boundary for every buffers in it
    fn assemble_and_write(
        &mut self,
        rows: i32,
        body_bytes: Vec<u8>,
        nodes: Vec<(i64, i64)>,
        segments: Vec<(i64, i64)>,
    ) -> Result<()> {
        let mut fbb = FlatBufferBuilder::new();

        let mut fb_nodes = Vec::new();
        for (len, null_count) in nodes {
            let mut node_builder = FbMetaNodeBuilder::new(&mut fbb);
            node_builder.push_length(len);
            node_builder.push_null_count(null_count);
            fb_nodes.push(node_builder.finish());
        }

        let mut fb_segments = Vec::new();
        for (offset, length) in segments {
            let mut segment_builder = FbMetaSegmentBuilder::new(&mut fbb);
            segment_builder.push_offset(offset);
            segment_builder.push_length(length);
            fb_segments.push(segment_builder.finish());
        }

        let header_offset = {
            let nodes_vec = fbb.create_vector(&fb_nodes);
            let segments_vec = fbb.create_vector(&fb_segments);

            let mut batch_builder = FbRecordBatchBuilder::new(&mut fbb);
            batch_builder.push_length(rows);
            batch_builder.push_nodes(nodes_vec);
            batch_builder.push_segments(segments_vec);
            if self.compress {
                batch_builder.push_compression(1 /* Zstd */);
            }
            batch_builder.finish()
        };
        let union_header = header_offset.as_union_value();

        let mut message_builder = FbMessageBuilder::new(&mut fbb);
        message_builder.push_version(1);
        message_builder.push_header_type(2); // 2 = Record Batch
        message_builder.push_header(union_header);
        message_builder.push_body_length(body_bytes.len() as i64);
        let root = message_builder.finish();

        fbb.finish(root, None);
        let meta_fb = fbb.finished_data();

        // Assemble and write the final IPC packet to the stream
        self.write_continuation(meta_fb.len())?;
        self.writer.write_all(meta_fb)?;

        // Metadata alignment padding to 8-byte boundary before body
        let written_bytes = 8 + meta_fb.len();
        self.write_padding(Self::align(written_bytes) - written_bytes)?;

        // Write the contiguous raw body bytes
        self.writer.write_all(&body_bytes)?;
        Ok(())
    }

    /// Writes a schema to an IPC message, returning metadata written
    fn write_schema(&mut self, schema: &Schema) -> Result<usize> {
        let mut fbb = FlatBufferBuilder::new();

        let schema_offset = schema_to_fb_offset(&mut fbb, schema);
        let union_header = schema_offset.as_union_value();

        let mut message_builder = FbMessageBuilder::new(&mut fbb);
        message_builder.push_version(1);
        message_builder.push_header_type(1); // 1 = Schema
        message_builder.push_header(union_header);
        message_builder.push_body_length(0);
        let root = message_builder.finish();

        fbb.finish(root, None);
        let bytes = fbb.finished_data();

        let len = bytes.len();
        let padded_header_len = Self::align(8 + len);
        let padded_metadata_len = padded_header_len - 8;
        let padding_needed = padded_metadata_len - len;

        self.write_continuation(padded_metadata_len)?;
        self.writer.write_all(bytes)?;
        self.write_padding(padding_needed)?;

        Ok(padded_header_len)
    }

    /// Writes the IPC continuation marker and metadata length prefix.
    fn write_continuation(&mut self, metadata_len: usize) -> Result<()> {
        let mut buffer = [0u8; 8];
        buffer[..4].copy_from_slice(&CONTINUATION_MARKER);
        buffer[4..].copy_from_slice(&(metadata_len as i32).to_le_bytes());
        self.writer.write_all(&buffer)?;
        Ok(())
    }

    /// Zero-allocation padding writer
    fn write_padding(&mut self, len: usize) -> Result<()> {
        if len > 0 {
            self.writer.write_all(&PADDING[..len])?;
        }
        Ok(())
    }
}

const MAX_PREALLOC_BYTES: usize = 64 * 1024 * 1024;

/// Reads exactly `len` bytes of message body, without reserving `len` before reading it.
pub fn read_bounded<R: Read>(reader: &mut R, len: usize) -> Result<Buffer> {
    let mut buf = Buffer::from_zeroed(len.min(MAX_PREALLOC_BYTES));
    let mut filled = 0;
    while filled < len {
        let end = buf.len();
        reader.read_exact(&mut buf.as_slice_mut()[filled..end])?;
        filled = end;
        if filled < len {
            buf.resize(len.min(end.saturating_mul(2)), 0);
        }
    }
    Ok(buf)
}

/// Read [`FbMessage`] from a reader while re-using a buffer for metadata
struct MessageReader<R: Read> {
    reader: R,
    buf: Vec<u8>,
}

impl<R: Read> MessageReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
        }
    }

    /// Reads the entire next message from the underlying reader which includes
    /// the metadata length, the metadata, and the body
    /// # Returns
    /// - `Ok(None)` if the the reader signals the end of stream with EOF on
    ///   the first read
    /// - `Err(_)` if the reader returns an error other than on the first
    ///   read, or if the metadata length is invalid
    /// - `Ok(Some(FbMessage, Buffer))` with the Header Message and Buffer contains the
    ///   body bytes
    pub fn try_next(&mut self) -> Result<Option<(FbMessage<'_>, Buffer)>> {
        let meta_len = self.read_meta_len()?;
        let Some(meta_len) = meta_len else {
            return Ok(None);
        };

        self.buf.clear();
        let read = (&mut self.reader)
            .take(meta_len as u64)
            .read_to_end(&mut self.buf)?;

        if read != meta_len {
            return Err(anyhow!(
                "Unexpected end of stream: expected {meta_len} metadata bytes, got {read}"
            ));
        }

        let message = root::<FbMessage>(&self.buf.as_slice())?;
        let body_length = usize::try_from(message.body_length())?;
        let buf = read_bounded(&mut self.reader, body_length)?;

        Ok(Some((message, buf)))
    }

    /// Read the metadata length for the next message from the underlying stream.
    fn read_meta_len(&mut self) -> Result<Option<usize>> {
        let mut meta_len: [u8; 4] = [0; 4];
        match self.reader.read_exact(&mut meta_len) {
            Ok(_) => {}
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(e.into())
                };
            }
        };
        let meta_len = {
            if meta_len == CONTINUATION_MARKER {
                self.reader.read_exact(&mut meta_len)?
            }
            i32::from_le_bytes(meta_len)
        };
        if meta_len == 0 {
            return Ok(None);
        }
        let meta_len = usize::try_from(meta_len)?;
        Ok(Some(meta_len))
    }
}

struct RecordBatchDecoder<'a> {
    buf: &'a Buffer,
    _fb_batch: FbRecordBatch<'a>,
    schema: SchemaRef,
    _nodes: VectorIter<'a, ForwardsUOffset<FbMetaNode<'a>>>,
    segments: VectorIter<'a, ForwardsUOffset<FbMetaSegment<'a>>>,
    is_compressed: bool,
    row_count: i32,
}

impl<'a> RecordBatchDecoder<'a> {
    pub fn new(buf: &'a Buffer, fb_batch: FbRecordBatch<'a>, schema: SchemaRef) -> Self {
        let segments = fb_batch.segments().unwrap().iter();
        let nodes = fb_batch.nodes().unwrap().iter();
        let is_compressed = fb_batch.compression() != 0;
        let row_count = fb_batch.length();
        Self {
            buf,
            _fb_batch: fb_batch,
            schema,
            _nodes: nodes,
            segments,
            is_compressed,
            row_count,
        }
    }

    /// Pulls and decompresses (if Zstd active) the next raw memory buffer segment
    fn next_buffer(&mut self) -> Result<Option<Buffer>> {
        let segment = self
            .segments
            .next()
            .ok_or_else(|| anyhow!("Malformed IPC stream: missing expected segment buffer"))?;

        let length = segment.length() as usize;
        if length == 0 {
            return Ok(None);
        }

        let offset = segment.offset() as usize;
        let raw_slice = self.buf.slice(offset, length);

        if self.is_compressed {
            let uncompressed_len = unsafe { raw_slice.get_uncheck::<i64>(0) } as usize;

            if uncompressed_len == usize::MAX {
                // -1 as signed 64-bit int
                Ok(Some(raw_slice.slice(8, length - 8)))
            } else {
                // Decompress the payload starting from byte offset 8
                let compressed_payload = &raw_slice.as_slice()[8..];
                let decompressed = zstd::stream::decode_all(compressed_payload)?;
                assert_eq!(decompressed.len(), uncompressed_len);
                Ok(Some(Buffer::from(decompressed)))
            }
        } else {
            Ok(Some(raw_slice))
        }
    }

    pub fn create_array(&mut self, field: &Field) -> Result<ArrayRef> {
        let validity_buf = self.next_buffer()?;
        let nulls = validity_buf
            .map(|v_buf| NullBuffer::new(BooleanBuffer::new(v_buf, 0, self.row_count as usize)));

        match field.data_type {
            DataType::Boolean => {
                let val_buf = self.next_buffer()?.ok_or_else(|| {
                    anyhow!("Malformed IPC: missing values buffer for BooleanArray")
                })?;
                let boolean_buf = BooleanBuffer::new(val_buf, 0, self.row_count as usize);
                Ok(Arc::new(BooleanArray::new(boolean_buf, nulls)))
            }
            DataType::Utf8 => {
                let offset_buf = self.next_buffer()?.ok_or_else(|| {
                    anyhow!("Malformed IPC: missing offsets buffer for StringArray")
                })?;
                let data_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Malformed IPC: missing data buffer for StringArray"))?;
                Ok(Arc::new(StringArray::new(offset_buf, data_buf, nulls)))
            }
            // Primitive numerical types
            DataType::Int8 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<i8>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
            DataType::Int16 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<i16>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
            DataType::Int32 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<i32>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
            DataType::Int64 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<i64>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
            DataType::Float32 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<f32>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
            DataType::Float64 => {
                let val_buf = self
                    .next_buffer()?
                    .ok_or_else(|| anyhow!("Missing values buffer"))?;
                Ok(Arc::new(PrimitiveArray::<f64>::new(
                    field.data_type,
                    val_buf,
                    nulls,
                )))
            }
        }
    }

    pub fn try_decode(&mut self) -> Result<RecordBatch> {
        let schema = Arc::clone(&self.schema);
        let mut columns = Vec::with_capacity(schema.fields.iter().count());
        for field in schema.fields.iter() {
            columns.push(self.create_array(field)?);
        }

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Reader<R: Read> {
    reader: MessageReader<R>,
    schema: SchemaRef,
    /// finished=true indicates we already reached to the end of a stream
    finished: bool,
}

impl<R: Read> Reader<R> {
    /// Returns the schema of this stream
    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn try_new(reader: R) -> Result<Self> {
        let mut msg_reader = MessageReader::new(reader);
        let message = msg_reader.try_next()?;

        let Some((message, _)) = message else {
            return Err(anyhow!("Expected schema message, found empty stream."));
        };

        if message.header_type() != /* Schema */ 1 {
            return Err(anyhow!(
                "Expected a schema as the first message in the stream, got: {:?}",
                message.header_type()
            ));
        }

        // For schema message, the schema is written in the header block
        let fb_schema = message
            .header_as_fb_schema()
            .ok_or_else(|| anyhow!("Failed to parse schema from message header"))?;

        let schema = fb_to_schema(fb_schema);

        Ok(Self {
            reader: msg_reader,
            schema: Arc::new(schema),
            finished: false,
        })
    }

    pub fn try_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let message = self.reader.try_next()?;
            let Some((message, body)) = message else {
                // we already reached to the end of the stream
                self.finished = true;
                return Ok(None);
            };

            match message.header_type() {
                /* Schema */
                1 => return Err(anyhow!("expected body bytes, but found schema")),
                /* Record Batch */
                2 => {
                    let batch_header = message
                        .header_as_fb_record_batch()
                        .ok_or_else(|| anyhow!("Unable to read IPC message as record batch"))?;

                    let mut decoder =
                        RecordBatchDecoder::new(&body, batch_header, self.schema.clone());
                    let batch = decoder.try_decode()?;
                    return Ok(Some(batch));
                }
                e => return Err(anyhow!("corrupted: unrecognise header type: {e}")),
            };
        }
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::PrimitiveArray;
    use crate::arrow::{Array, DataType, Field, Schema};
    use std::sync::Arc;

    pub unsafe fn root_as_message(
        buf: &[u8],
    ) -> Result<crate::arrow::ipc::FbMessage<'_>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<crate::arrow::ipc::FbMessage>(buf)
    }

    #[test]
    fn test_record_batch_writer_roundtrip() {
        // 1. Define Schema: [id: Int32]
        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        // 2. Build column id = [1000, 2000, 3000, 4000] (4 * i32 = 16 bytes)
        let id_column: crate::arrow::ArrayRef =
            Arc::new(PrimitiveArray::from(vec![1000i32, 2000, 3000, 4000]));
        let batch = RecordBatch::try_new(schema, vec![id_column]).unwrap();

        // 3. Serialize utilizing Writer into a Vec<u8> buffer (with Zstd compression enabled!)
        let mut buffer = Vec::new();
        {
            let mut writer = Writer::new(&mut buffer);
            writer.write(&batch).unwrap();
        }

        // 4. Decode and Verify the serialized packet
        assert!(buffer.len() > 8);

        // --- SKIP SCHEMA MESSAGE BLOCK ---
        assert_eq!(&buffer[..4], &[0xFF, 0xFF, 0xFF, 0xFF]); // Schema continuation
        let schema_meta_len = i32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize;
        let schema_meta_bytes = &buffer[8..8 + schema_meta_len];

        // Parse first message as FbMessage (Schema)
        let fb_schema_msg = unsafe { root_as_message(schema_meta_bytes).unwrap() };
        assert_eq!(fb_schema_msg.header_type(), 1); // 1 = Schema
        assert_eq!(fb_schema_msg.body_length(), 0); // Schemas have no body

        let schema_message_size = Writer::<Vec<u8>>::align(8 + schema_meta_len);

        // --- READ RECORDBATCH MESSAGE BLOCK ---
        let batch_msg = &buffer[schema_message_size..];
        assert_eq!(&batch_msg[..4], &[0xFF, 0xFF, 0xFF, 0xFF]); // Batch continuation

        let metadata_len = i32::from_le_bytes(batch_msg[4..8].try_into().unwrap()) as usize;
        let metadata_bytes = &batch_msg[8..8 + metadata_len];

        // Parse second message as FbMessage (RecordBatch)
        let fb_msg = unsafe { root_as_message(metadata_bytes).unwrap() };
        assert_eq!(fb_msg.header_type(), 2); // 2 = RecordBatch
        assert!(fb_msg.body_length() > 0);

        // Extract the inner FbRecordBatch table!
        let header_table = fb_msg.header().unwrap();
        let fb_batch = crate::arrow::ipc::FbRecordBatch::from_table(header_table);
        assert_eq!(fb_batch.length(), 4);
        assert_eq!(fb_batch.compression(), 1); // 1 = Zstd

        let fb_nodes = fb_batch.nodes().unwrap();
        assert_eq!(fb_nodes.len(), 1);
        assert_eq!(fb_nodes.get(0).length(), 4);
        assert_eq!(fb_nodes.get(0).null_count(), 0);

        let fb_segments = fb_batch.segments().unwrap();
        // 1 segment for validity null bitmap (which is empty length 0) and 1 for values
        assert_eq!(fb_segments.len(), 2);

        let nulls_segment = fb_segments.get(0);
        assert_eq!(nulls_segment.length(), 0); // null bitmap is empty

        let values_segment = fb_segments.get(1);
        assert!(values_segment.length() > 0);

        // 5. Decompress and verify values in the body
        let body_start = Writer::<Vec<u8>>::align(8 + metadata_len);
        let body_bytes = &batch_msg[body_start..];

        let val_offset = values_segment.offset() as usize;
        let val_len = values_segment.length() as usize;
        let compressed_val_bytes = &body_bytes[val_offset..val_offset + val_len];

        // Read uncompressed length prefix (i64, little-endian) per spec
        let uncompressed_size = i64::from_le_bytes(compressed_val_bytes[0..8].try_into().unwrap());

        let decompressed_bytes = if uncompressed_size == -1 {
            // Buffer was left uncompressed per spec! Read the raw 16 bytes starting at offset 8
            compressed_val_bytes[8..].to_vec()
        } else {
            assert_eq!(uncompressed_size, 16);
            let compressed_payload = &compressed_val_bytes[8..];
            zstd::stream::decode_all(compressed_payload).unwrap()
        };

        assert_eq!(decompressed_bytes.len(), 16);

        // Cast uncompressed bytes directly back to i32 values (Zero-Copy)
        let decompressed_buffer = Buffer::from(decompressed_bytes);
        unsafe {
            assert_eq!(decompressed_buffer.get_uncheck::<i32>(0), 1000);
            assert_eq!(decompressed_buffer.get_uncheck::<i32>(1), 2000);
            assert_eq!(decompressed_buffer.get_uncheck::<i32>(2), 3000);
            assert_eq!(decompressed_buffer.get_uncheck::<i32>(3), 4000);
        }
    }

    #[test]
    fn test_record_batch_writer_reader_roundtrip() {
        use crate::arrow::array::{BooleanArray, StringArray};

        // 1. Define Diverse Schema: [id: Int32, name: Utf8, active: Boolean]
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
            },
        ]));

        // 2. Build mock columns
        let id_column: ArrayRef = Arc::new(PrimitiveArray::from(vec![1i32, 2, 3]));
        let name_column: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Bob")]));
        let active_column: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));

        let original_batch =
            RecordBatch::try_new(schema.clone(), vec![id_column, name_column, active_column])
                .unwrap();

        // 3. Serialize utilizing StreamWriter into a Vec<u8> stream buffer
        let mut stream_buffer = Vec::new();
        {
            let mut writer = Writer::new(&mut stream_buffer);
            writer.write(&original_batch).unwrap();
        }

        // 4. Deserialize utilizing StreamReader (eagerly consumes Schema)
        let mut reader = Reader::try_new(&stream_buffer[..]).unwrap();

        // Assert Schema matches
        assert_eq!(reader.schema().fields.iter().count(), 3);
        assert_eq!(reader.schema().fields.iter().nth(1).unwrap().name, "name");

        // Read next batch
        let opt_batch = reader.try_next().unwrap();
        assert!(opt_batch.is_some());

        let deserialized_batch = opt_batch.unwrap();
        assert_eq!(deserialized_batch.num_rows(), 3);
        assert_eq!(deserialized_batch.num_columns(), 3);

        // Verify values are preserved exactly
        let id_col = deserialized_batch
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = deserialized_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert!(name_col.is_null(1));
        assert_eq!(name_col.value(2), "Bob");

        let active_col = deserialized_batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_col.value(0));
        assert!(!active_col.value(1));
        assert!(active_col.value(2));

        // Next batch should return None (clean EOF)
        assert!(reader.try_next().unwrap().is_none());
    }

    #[test]
    fn test_record_batch_reader_iterator() {
        use crate::arrow::array::StringArray;

        // 1. Define Schema: [id: Int32, label: Utf8]
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "label".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ]));

        // 2. Build 2 mock batches
        let id_column1: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20]));
        let label_column1: ArrayRef = Arc::new(StringArray::from(vec!["A", "B"]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![id_column1, label_column1]).unwrap();

        let id_column2: ArrayRef = Arc::new(PrimitiveArray::from(vec![30i32, 40]));
        let label_column2: ArrayRef = Arc::new(StringArray::from(vec!["C", "D"]));
        let batch2 = RecordBatch::try_new(schema.clone(), vec![id_column2, label_column2]).unwrap();

        // 3. Serialize both batches into a stream buffer using StreamWriter
        let mut stream_buffer = Vec::new();
        {
            let mut writer = Writer::new(&mut stream_buffer);
            writer.write(&batch1).unwrap();
            writer.write(&batch2).unwrap();
        }

        // 4. Deserialize utilizing StreamReader as an Iterator!
        let reader = Reader::try_new(&stream_buffer[..]).unwrap();

        // Collect the batches using standard Iterator::collect!
        let batches: Result<Vec<RecordBatch>> = reader.collect();
        let batches = batches.unwrap();

        assert_eq!(batches.len(), 2);

        // Verify batch 1 values
        let b1 = &batches[0];
        assert_eq!(b1.num_rows(), 2);
        let id_col1 = b1
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(id_col1.value(0), 10);
        assert_eq!(id_col1.value(1), 20);

        // Verify batch 2 values
        let b2 = &batches[1];
        assert_eq!(b2.num_rows(), 2);
        let id_col2 = b2
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(id_col2.value(0), 30);
        assert_eq!(id_col2.value(1), 40);
    }
}

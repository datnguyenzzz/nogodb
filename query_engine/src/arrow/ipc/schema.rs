use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::arrow::{Buffer, Field, Schema, ipc::build_ipc_data_type};

/// create IPC Field from arrow::Field
pub fn build_ipc_field(mut fbb: FlatBufferBuilder, field: &Field) -> WIPOffset<Field> {
    let field_name = fbb.create_string(field.name.as_str());
    let field_type = build_ipc_data_type(fbb, &field.data_type);

    todo!()
}

/// Convert arrow::Schema <--> IPC Schema
pub struct IpcSchema {
    pub buffer: Buffer,
}

impl IpcSchema {
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            buffer: Buffer::from(vec),
        }
    }

    pub fn from_schema(schema: &Schema) -> Self {
        todo!("implement me")
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.buffer.data.to_vec()
    }

    pub fn schema(&self) -> Schema {
        todo!()
    }

    pub fn num_fields(&self) -> usize {
        todo!()
    }

    pub fn get_field(&self, index: usize) -> Option<Field> {
        todo!()
    }
}

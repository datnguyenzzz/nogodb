use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, WIPOffset, root};

use crate::arrow::{
    Field, Schema,
    ipc::{FbField, FbFieldBuilder, FbSchema, FbSchemaBuilder, build_ipc_data_type},
};

/// create IPC Field from arrow::Field
pub fn build_ipc_field<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    field: &Field,
) -> WIPOffset<FbField<'a>> {
    let field_name = fbb.create_string(field.name.as_str());
    let (type_type, field_type) = build_ipc_data_type(fbb, &field.data_type);
    let mut builder = FbFieldBuilder::new(fbb);
    builder.push_name(field_name);
    builder.push_nullable(field.nullable);
    builder.push_type_type(type_type);
    builder.push_type(field_type);
    builder.finish()
}

/// Serialize a schema in IPC format, returning a completed [`FlatBufferBuilder`]
/// Note: Call [`FlatBufferBuilder::finished_data`] to get the serialized bytes
pub fn schema_to_fb<'fbb>(schema: &Schema) -> FlatBufferBuilder<'fbb> {
    let mut fbb = FlatBufferBuilder::new();
    let root = schema_to_fb_offset(&mut fbb, schema);
    fbb.finish(root, None);
    fbb
}

/// Serialize a schema in IPC format, returning the in progress offset
pub fn schema_to_fb_offset<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    schema: &Schema,
) -> WIPOffset<FbSchema<'fbb>> {
    let fb_fields = schema
        .fields
        .iter()
        .map(|field| build_ipc_field(fbb, field))
        .collect::<Vec<_>>();

    let fb_fields_list = fbb.create_vector(&fb_fields);
    let mut builder = FbSchemaBuilder::new(fbb);
    builder.push_fields(fb_fields_list);
    builder.finish()
}

/// Verifies that a buffer of bytes contains a `Schema` and returns it.
pub fn root_as_schema(buf: &[u8]) -> Result<FbSchema<'_>, InvalidFlatbuffer> {
    root::<FbSchema>(buf)
}

/// Deserialize an IPC [`FbSchema`] from flat buffers to an arrow [Schema].
pub fn fb_to_schema(fb_schema: FbSchema) -> Schema {
    let mut fields: Vec<FbField> = vec![];
    let fb_fields = fb_schema.fields().unwrap_or_default();
    for fb_field in fb_fields {
        fields.push(fb_field.into())
    }

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::DataType;

    #[test]
    fn test_schema_serialization_roundtrip() {
        // 1. Construct a standard Schema with various DataTypes and nullability
        let original_schema = Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "username".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
            },
            Field {
                name: "rating".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]);

        // 2. Serialize to FlatBuffers format
        let fbb = schema_to_fb(&original_schema);
        let serialized_bytes = fbb.finished_data();

        // 3. Deserialize back via unsafe root verification
        let fb_schema = root_as_schema(serialized_bytes).unwrap();
        let deserialized_schema = fb_to_schema(fb_schema);

        // 4. Validate that all schema properties and field metadata are preserved exactly
        assert_eq!(
            deserialized_schema.fields.iter().count(),
            original_schema.fields.iter().count()
        );

        for (orig, deser) in original_schema
            .fields
            .iter()
            .zip(deserialized_schema.fields.iter())
        {
            assert_eq!(orig.name, deser.name);
            assert_eq!(orig.data_type, deser.data_type);
            assert_eq!(orig.nullable, deser.nullable);
        }
    }
}

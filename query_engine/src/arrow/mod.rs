pub mod ipc;

use std::sync::Arc;

pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float16,
    Float32,
    Float64,
    Utf8,
}

/// Describes a single column in a [`Schema`](super::Schema).
///
/// A [`Schema`](super::Schema) is an ordered collection of
/// [`Field`] objects. Fields contain:
/// * `name`: the name of the field
/// * `data_type`: the type of the field
/// * `nullable`: if the field is nullable
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub type FieldRef = Arc<Field>;

pub struct Fields(Arc<[FieldRef]>);

impl Default for Fields {
    fn default() -> Self {
        Self(Arc::new([]))
    }
}

pub struct Schema {
    pub fields: Fields,
}

impl Schema {
    pub fn empty() -> Self {
        Self {
            fields: Default::default(),
        }
    }

    pub fn new(fields: impl Into<Fields>) -> Self {
        Self {
            fields: fields.into(),
        }
    }
}

/// Immutable buffer that is shared across threads
pub struct Buffer {
    pub data: Arc<Vec<u8>>,
}

impl Buffer {
    /// Create a [`Buffer`] from the provided [`Vec`] without copying
    pub fn from(vec: Vec<u8>) -> Self {
        Self {
            data: Arc::from(vec),
        }
    }
}

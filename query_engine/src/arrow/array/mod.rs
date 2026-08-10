pub mod boolean_array;
pub mod primitive_array;
pub mod string_array;
pub use boolean_array::BooleanArray;
pub use primitive_array::PrimitiveArray;
pub use string_array::StringArray;

use crate::arrow::DataType;

/// Trait implemented by native Rust primitive types that can be stored in `PrimitiveArray`
pub trait NativeType: Send + Sync + Copy + 'static {
    fn data_type() -> DataType;
}

macro_rules! define_native_types {
    ($(
        ($ty:ty, $dt:path)
    ),* $(,)?) => {
        $(
            impl NativeType for $ty {
                #[inline]
                fn data_type() -> DataType {
                    $dt
                }
            }
        )*
    };
}

define_native_types! {
    (bool, DataType::Boolean),
    (i8, DataType::Int8),
    (i16, DataType::Int16),
    (i32, DataType::Int32),
    (i64, DataType::Int64),
    (f32, DataType::Float32),
    (f64, DataType::Float64),
}

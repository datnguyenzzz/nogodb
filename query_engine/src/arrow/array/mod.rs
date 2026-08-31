pub mod boolean_array;
pub mod primitive_array;
pub mod select;
pub mod string_array;
pub use boolean_array::BooleanArray;
pub use primitive_array::PrimitiveArray;
pub use string_array::StringArray;

use crate::arrow::DataType;

/// Trait implemented by native Rust primitive types that can be stored in `PrimitiveArray`
pub trait NativeType: Send + Sync + Copy + 'static {
    fn data_type() -> DataType;
    fn default() -> Self;
}

macro_rules! define_native_types {
    ($(
        ($ty:ty, $dt:path, $default:expr)
    ),* $(,)?) => {
        $(
            impl NativeType for $ty {
                #[inline]
                fn data_type() -> DataType {
                    $dt
                }
                #[inline]
                fn default() -> Self {
                    $default
                }
            }
        )*
    };
}

/// Matches a runtime `DataType` enum variant and dispatches a generic closure/block
/// bound to the correct, physical concrete Rust type.
#[macro_export]
macro_rules! dispatch_native_type {
    ($data_type:expr, $type_var:ident, $body:block) => {
        match $data_type {
            $crate::arrow::DataType::Int8 => {
                type $type_var = i8;
                $body
            }
            $crate::arrow::DataType::Int16 => {
                type $type_var = i16;
                $body
            }
            $crate::arrow::DataType::Int32 => {
                type $type_var = i32;
                $body
            }
            $crate::arrow::DataType::Int64 => {
                type $type_var = i64;
                $body
            }
            $crate::arrow::DataType::Float32 => {
                type $type_var = f32;
                $body
            }
            $crate::arrow::DataType::Float64 => {
                type $type_var = f64;
                $body
            }
            $crate::arrow::DataType::Boolean => {
                type $type_var = bool;
                $body
            }
            other => panic!("DataType {:?} is not a native primitive type", other),
        }
    };
}

define_native_types! {
    (bool, DataType::Boolean, false),
    (i8, DataType::Int8, 0i8),
    (i16, DataType::Int16, 0i16),
    (i32, DataType::Int32, 0i32),
    (i64, DataType::Int64, 0i64),
    (f32, DataType::Float32, 0.0f32),
    (f64, DataType::Float64, 0.0f64),
}

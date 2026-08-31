use std::sync::Arc;

use anyhow::{Result, anyhow, bail};

use crate::{
    arrow::{
        Array, ArrayRef, DataType,
        array::{BooleanArray, PrimitiveArray, StringArray},
    },
    dispatch_native_type,
};

/// Take elements by index from [`Array`], creating a new [`Array`] from those indexes.
///
/// ```text
/// ┌─────────────────┐      ┌─────────┐                              ┌─────────────────┐
/// │        A        │      │    0    │                              │        A        │
/// ├─────────────────┤      ├─────────┤                              ├─────────────────┤
/// │        D        │      │    2    │                              │        B        │
/// ├─────────────────┤      ├─────────┤   take(values, indices)      ├─────────────────┤
/// │        B        │      │    3    │ ─────────────────────────▶   │        C        │
/// ├─────────────────┤      ├─────────┤                              ├─────────────────┤
/// │        C        │      │    1    │                              │        D        │
/// ├─────────────────┤      └─────────┘                              └─────────────────┘
/// │        E        │
/// └─────────────────┘
///    values array          indices array                              result
/// ```
pub fn take(values: &dyn Array, indexes: &dyn Array) -> Result<ArrayRef> {
    if indexes.data_type() != &DataType::Int32 {
        bail!(
            "Take indices must be of DataType::Int32, but found {:?}",
            indexes.data_type()
        )
    }

    let indexes_arr = indexes
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .ok_or_else(|| anyhow!("Failed to downcast indices to PrimitiveArray<i32>"))?;

    match values.data_type() {
        DataType::Boolean => {
            let typed_values = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("Expected StringArray"))?;
            Ok(Arc::new(typed_values.take(indexes_arr)?))
        }
        DataType::Utf8 => {
            let typed_values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Expected BooleanArray"))?;
            Ok(Arc::new(typed_values.take(indexes_arr)?))
        }

        primitive => {
            dispatch_native_type!(primitive, T, {
                let typed_values = values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .ok_or_else(|| anyhow!("Expected PrimitiveArray"))?;
                Ok(Arc::new(typed_values.take(indexes_arr)?))
            })
        }
    }
}

// Other select functions here , filter, merger, coalesce

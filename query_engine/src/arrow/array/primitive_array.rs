// https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout

use std::{marker, mem, ptr, sync::Arc};

use crate::arrow::{Array, BooleanBuffer, Buffer, DataType, NullBuffer, array::NativeType};

/// An array of primitive values. A primitive value array represents an array of values
/// each having the same physical slot width typically measured in bytes
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer,
    nulls: Option<NullBuffer>,
    _phantom: marker::PhantomData<T>,
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    fn len(&self) -> usize {
        self.values.len() / mem::size_of::<T>()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> crate::arrow::ArrayRef {
        let size = mem::size_of::<T>();
        let offset_bytes = offset.checked_mul(size).expect("offset overflow");
        let length_bytes = length.checked_mul(size).expect("length overflow");
        Arc::new(Self {
            data_type: self.data_type,
            values: self.values.slice(offset_bytes, length_bytes),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
            _phantom: marker::PhantomData,
        })
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn new(data_type: DataType, values: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self {
            data_type,
            values,
            nulls,
            _phantom: marker::PhantomData,
        }
    }

    /// Returns the primitive value at index `index` with zero-copy casting
    pub fn value(&self, index: usize) -> T {
        assert!(
            index < self.len(),
            "Trying to access an element at index {} from a PrimitiveArray of length {}",
            index,
            self.len()
        );

        unsafe { self.values.get_uncheck(index) }
    }
}

impl<T: NativeType> From<Vec<T>> for PrimitiveArray<T> {
    fn from(value: Vec<T>) -> Self {
        // re-interpret the original Vec<T>'s memory block directly as a Vec<u8> without any copying
        let size = mem::size_of::<T>();
        let cap = value.capacity().saturating_mul(size);
        let len = value.len().saturating_mul(size);
        // Wrap the vector in ManuallyDrop to prevent it from freeing its memory when this function exits
        let mut value = mem::ManuallyDrop::new(value);
        let value_bytes = unsafe { Vec::from_raw_parts(value.as_mut_ptr() as *mut u8, len, cap) };
        Self::new(T::data_type(), Buffer::from(value_bytes), None)
    }
}

impl<T: NativeType> From<Vec<Option<T>>> for PrimitiveArray<T> {
    fn from(value: Vec<Option<T>>) -> Self {
        let len = value.len();
        let size = mem::size_of::<T>();

        let mut value_bytes: Vec<u8> = Vec::with_capacity(len.saturating_mul(size));
        let mut validity_bytes = vec![0u8; (len + 7) / 8]; //bit-packed validity array

        for (i, opt) in value.into_iter().enumerate() {
            match opt {
                Some(val) => {
                    // memcpy the val into the value_bytes
                    unsafe {
                        let src_ptr = &val as *const T as *const u8;
                        let dst_ptr = value_bytes.as_mut_ptr().add(value_bytes.len());
                        ptr::copy_nonoverlapping(src_ptr, dst_ptr, size);
                        value_bytes.set_len(value_bytes.len() + size);
                    }
                    // mark this slot is valid
                    validity_bytes[i / 8] |= 1 << (i % 8);
                }
                None => {
                    // Zero-copy, zero-allocation padding: write raw zero bytes directly into
                    // our pre-allocated `value_bytes` buffer to preserve 8-byte boundaries
                    unsafe {
                        let dst_ptr = value_bytes.as_mut_ptr().add(value_bytes.len());
                        ptr::write_bytes(dst_ptr, 0, size);
                        value_bytes.set_len(value_bytes.len() + size);
                    }
                }
            }
        }

        Self::new(
            T::data_type(),
            Buffer::from(value_bytes),
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity_bytes),
                0,
                len,
            ))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_array_un_nullable() {
        // Create un-nullable i32 array: [100, 200, 300]
        let original_vec = vec![100i32, 200, 300];
        let array = PrimitiveArray::from(original_vec);

        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Int32);
        assert!(array.nulls().is_none());

        assert_eq!(array.value(0), 100);
        assert_eq!(array.value(1), 200);
        assert_eq!(array.value(2), 300);
        assert!(!array.is_null(0));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_primitive_array_nullable() {
        // Create nullable f64 array: [Some(1.5), None, Some(3.5), None, Some(5.5)]
        let original_vec = vec![Some(1.5f64), None, Some(3.5), None, Some(5.5)];
        let array = PrimitiveArray::from(original_vec);

        assert_eq!(array.len(), 5);
        assert_eq!(array.data_type(), &DataType::Float64);
        assert!(array.nulls().is_some());

        let null_buf = array.nulls().unwrap();
        assert_eq!(null_buf.null_count(), 2); // Eagerly computed 2 nulls!

        // Assert elements
        assert_eq!(array.value(0), 1.5);
        assert!(array.is_null(1)); // Slot 1 is Null
        assert_eq!(array.value(1), 0.0); // Null padded to zero
        assert_eq!(array.value(2), 3.5);
        assert!(array.is_null(3)); // Slot 3 is Null
        assert_eq!(array.value(4), 5.5);
    }

    #[test]
    fn test_primitive_array_slicing() {
        // Create nullable i32 array: [10, None, 30, 40, None, 60] (Length: 6)
        let original_vec = vec![Some(10i32), None, Some(30), Some(40), None, Some(60)];
        let array = PrimitiveArray::from(original_vec);

        // Slice from index 2, length 3: [30, 40, None]
        let size = std::mem::size_of::<i32>();
        let sliced_array = PrimitiveArray::<i32> {
            data_type: array.data_type,
            values: array.values.slice(2 * size, 3 * size),
            nulls: array.nulls.as_ref().map(|n| n.slice(2, 3)),
            _phantom: std::marker::PhantomData,
        };

        assert_eq!(sliced_array.len(), 3);
        assert!(sliced_array.nulls().is_some());
        assert_eq!(sliced_array.nulls().unwrap().null_count(), 1); // 1 null in this slice!

        // Validate values are shifted correctly
        assert_eq!(sliced_array.value(0), 30);
        assert_eq!(sliced_array.value(1), 40);
        assert!(sliced_array.is_null(2)); // Original index 4 (now sliced 2) is Null
        assert_eq!(sliced_array.value(2), 0); // Null padded to zero
    }
}

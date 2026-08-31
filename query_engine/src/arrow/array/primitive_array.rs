// https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout

use std::{iter, marker, mem, ptr, sync::Arc};

use anyhow::Result;

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
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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

    fn buffers(&self) -> Vec<Buffer> {
        vec![self.values.clone()]
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

    pub fn iter(&self) -> PrimitiveIter<'_, T> {
        PrimitiveIter::new(self)
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

    pub fn take(&self, indexes: &PrimitiveArray<i32>) -> Result<Self> {
        let res = indexes
            .iter()
            .map(|idx_opt| match idx_opt {
                Some(idx) => {
                    let u_idx = idx as usize;
                    if u_idx >= self.len() {
                        return None;
                    }
                    if self.is_null(u_idx) {
                        None
                    } else {
                        Some(self.value(u_idx))
                    }
                }
                None => None,
            })
            .collect();

        Ok(res)
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

impl<T: NativeType> iter::FromIterator<T> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iterator = iter.into_iter();
        let (len, _) = iterator.size_hint();

        let mut values_vec = Vec::with_capacity(len);
        for val in iterator {
            values_vec.push(val)
        }

        let values_buf = Buffer::from(unsafe {
            let mut manual_vec = mem::ManuallyDrop::new(values_vec);
            Vec::from_raw_parts(
                manual_vec.as_mut_ptr() as *mut u8,
                manual_vec.len() * mem::size_of::<T>(),
                manual_vec.capacity() * mem::size_of::<T>(),
            )
        });

        Self::new(T::data_type(), values_buf, None)
    }
}

impl<T: NativeType> iter::FromIterator<Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iterator = iter.into_iter();
        let (len, _) = iterator.size_hint();

        let mut value_vec = Vec::with_capacity(len);
        let mut validity_bytes = Vec::with_capacity((len + 7) / 8); //bit-packed validity array

        let mut current_bytes = 0u8;
        let mut bit_count = 0;
        let mut has_nulls = false;

        for item in iterator {
            match item {
                Some(val) => {
                    value_vec.push(val);
                    current_bytes |= 1 << (bit_count % 8)
                }
                None => {
                    // Safe zeroed-allocation for the null element slot
                    value_vec.push(unsafe { mem::zeroed() });
                    has_nulls = true
                    // Leaves validity bit as 0 (representing Null)
                }
            }
            bit_count += 1;
            if bit_count % 8 == 0 {
                validity_bytes.push(current_bytes);
                current_bytes = 0u8;
            }
        }

        if bit_count % 8 != 0 {
            validity_bytes.push(current_bytes);
        }

        let values_buf = Buffer::from(unsafe {
            let mut manual_vec = mem::ManuallyDrop::new(value_vec);
            Vec::from_raw_parts(
                manual_vec.as_mut_ptr() as *mut u8,
                manual_vec.len() * mem::size_of::<T>(),
                manual_vec.capacity() * mem::size_of::<T>(),
            )
        });

        let nulls = if has_nulls {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity_bytes),
                0,
                bit_count,
            )))
        } else {
            None
        };

        Self::new(T::data_type(), values_buf, nulls)
    }
}

pub struct PrimitiveIter<'a, T: NativeType> {
    array: &'a PrimitiveArray<T>,
    current: usize,
    len: usize,
}

impl<'a, T: NativeType> PrimitiveIter<'a, T> {
    pub fn new(arr: &'a PrimitiveArray<T>) -> Self {
        Self {
            array: arr,
            current: 0,
            len: arr.len(),
        }
    }
}

impl<'a, T: NativeType> Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current < self.len {
            let old = self.current;
            self.current += 1;
            if self.array.is_null(old) {
                Some(None)
            } else {
                Some(Some(self.array.value(old)))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.current, Some(self.len - self.current))
    }
}

// Implement ExactSizeIterator for fast pre-allocations in vectorized loops
impl<'a, T: NativeType> ExactSizeIterator for PrimitiveIter<'a, T> {}

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

    #[test]
    fn test_primitive_array_take_and_iter() {
        // 1. Create source array: [10, None, 30, 40, None, 60]
        let original =
            PrimitiveArray::from(vec![Some(10i32), None, Some(30), Some(40), None, Some(60)]);

        // 2. Iterate and verify original values
        let gathered: Vec<Option<i32>> = original.iter().collect();
        assert_eq!(
            gathered,
            vec![Some(10), None, Some(30), Some(40), None, Some(60)]
        );

        // 3. Create indices array: [2, 0, null, 4, 3, 100] (note: 100 is out of bounds!)
        let indices =
            PrimitiveArray::from(vec![Some(2i32), Some(0), None, Some(4), Some(3), Some(100)]);

        // 4. Perform take!
        let taken = original.take(&indices).unwrap();

        assert_eq!(taken.len(), 6);
        let taken_gathered: Vec<Option<i32>> = taken.iter().collect();

        // Index 2 -> Some(30)
        // Index 0 -> Some(10)
        // Index null -> None
        // Index 4 -> None
        // Index 3 -> Some(40)
        // Index 100 (out of bounds) -> None
        assert_eq!(
            taken_gathered,
            vec![Some(30), Some(10), None, None, Some(40), None]
        );
    }
}

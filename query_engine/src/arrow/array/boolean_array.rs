use std::sync::Arc;

use anyhow::Result;

use crate::arrow::{Array, BooleanBuffer, Buffer, DataType, NullBuffer, array::PrimitiveArray};

pub struct BooleanArray {
    values: BooleanBuffer,
    nulls: Option<NullBuffer>,
}

impl Array for BooleanArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> crate::arrow::ArrayRef {
        Arc::new(Self {
            values: self.values.slice(offset, length),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
        })
    }

    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    fn buffers(&self) -> Vec<Buffer> {
        vec![self.values.buffer.clone()]
    }
}

impl BooleanArray {
    pub fn new(values: BooleanBuffer, nulls: Option<NullBuffer>) -> Self {
        Self { values, nulls }
    }

    pub fn value(&self, index: usize) -> bool {
        assert!(
            index < self.len(),
            "Trying to access an element at index {} from a BooleanArray of length {}",
            index,
            self.len()
        );

        self.values.value(index)
    }

    pub fn iter(&self) -> BooleanIter<'_> {
        BooleanIter::new(self)
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

    /// Performs a bitwise binary operation with another [`BooleanArray`] returning a new [`BooleanArray`].
    pub fn bitwise_bin_op<F>(&self, rhs: &BooleanArray, op: F) -> BooleanArray
    where
        F: FnMut(u64, u64) -> u64,
    {
        assert_eq!(self.len(), rhs.len(), "BooleanArray must have equal length");

        // Apply bitwise operation to the values buffer
        let new_values = self.values.bitwise_bin_op(&rhs.values, op);

        // Merge validity bitmaps: output is null if either input is null (Standard Null Propagation)
        let new_nulls = match (&self.nulls, &rhs.nulls) {
            (Some(n1), Some(n2)) => {
                let validity_buf = n1.inner().bitwise_bin_op(n2.inner(), |a, b| a & b);
                Some(NullBuffer::new(validity_buf))
            }
            (Some(n1), None) => Some(n1.clone()),
            (None, Some(n2)) => Some(n2.clone()),
            (None, None) => None,
        };

        BooleanArray::new(new_values, new_nulls)
    }
}

pub struct BooleanIter<'a> {
    array: &'a BooleanArray,
    current: usize,
    len: usize,
}

impl<'a> BooleanIter<'a> {
    pub fn new(arr: &'a BooleanArray) -> Self {
        Self {
            array: arr,
            current: 0,
            len: arr.len(),
        }
    }
}

impl<'a> Iterator for BooleanIter<'a> {
    type Item = Option<bool>;

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

impl<'a> ExactSizeIterator for BooleanIter<'a> {}

impl std::iter::FromIterator<bool> for BooleanArray {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let iterator = iter.into_iter();
        let (len, _) = iterator.size_hint();
        let mut value_bytes = vec![0u8; (len + 7) / 8];
        let mut count = 0;
        for val in iterator {
            if val {
                value_bytes[count / 8] |= 1 << (count % 8);
            }
            count += 1;
        }
        Self {
            values: BooleanBuffer::new(Buffer::from(value_bytes), 0, count),
            nulls: None,
        }
    }
}

impl std::iter::FromIterator<Option<bool>> for BooleanArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let iterator = iter.into_iter();
        let (len, _) = iterator.size_hint();
        let mut value_bytes = vec![0u8; (len + 7) / 8];
        let mut validity_bytes = Vec::with_capacity((len + 7) / 8);

        let mut current_bytes = 0u8;
        let mut bit_count = 0;
        let mut has_nulls = false;

        for item in iterator {
            match item {
                Some(val) => {
                    if val {
                        value_bytes[bit_count / 8] |= 1 << (bit_count % 8);
                    }
                    current_bytes |= 1 << (bit_count % 8);
                }
                None => {
                    has_nulls = true;
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

        let nulls = if has_nulls {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity_bytes),
                0,
                bit_count,
            )))
        } else {
            None
        };

        Self {
            values: BooleanBuffer::new(Buffer::from(value_bytes), 0, bit_count),
            nulls,
        }
    }
}

impl From<Vec<bool>> for BooleanArray {
    fn from(value: Vec<bool>) -> Self {
        let len = value.len();
        let mut value_bytes = vec![0u8; (len + 7) / 8];

        for (i, val) in value.into_iter().enumerate() {
            if val {
                value_bytes[i / 8] |= 1 << (i % 8);
            }
        }

        Self {
            values: BooleanBuffer::new(Buffer::from(value_bytes), 0, len),
            nulls: None,
        }
    }
}

impl From<Vec<Option<bool>>> for BooleanArray {
    fn from(value: Vec<Option<bool>>) -> Self {
        let len = value.len();
        let mut value_bytes = vec![0u8; (len + 7) / 8];
        let mut validity_bytes = vec![0u8; (len + 7) / 8];

        for (i, opt) in value.into_iter().enumerate() {
            if opt.is_some() {
                if opt.unwrap() {
                    value_bytes[i / 8] |= 1 << (i % 8);
                }

                validity_bytes[i / 8] |= 1 << (i % 8);
            }
        }

        Self {
            values: BooleanBuffer::new(Buffer::from(value_bytes), 0, len),
            nulls: Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity_bytes),
                0,
                len,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_array_un_nullable() {
        let original_vec = vec![true, false, true, true, false];
        let array = BooleanArray::from(original_vec);

        assert_eq!(array.len(), 5);
        assert_eq!(array.data_type(), &DataType::Boolean);
        assert!(array.nulls().is_none());

        assert!(array.value(0));
        assert!(!array.value(1));
        assert!(array.value(2));
        assert!(array.value(3));
        assert!(!array.value(4));
    }

    #[test]
    fn test_boolean_array_nullable() {
        let original_vec = vec![Some(true), None, Some(false), None, Some(true)];
        let array = BooleanArray::from(original_vec);

        assert_eq!(array.len(), 5);
        assert_eq!(array.data_type(), &DataType::Boolean);
        assert!(array.nulls().is_some());

        let null_buf = array.nulls().unwrap();
        assert_eq!(null_buf.null_count(), 2); // Eagerly computed 2 nulls!

        assert!(array.value(0));
        assert!(array.is_null(1));
        assert!(!array.value(1)); // Null yields default (false)
        assert!(!array.value(2)); // Valid false
        assert!(array.is_null(3));
        assert!(array.value(4));
    }

    #[test]
    fn test_boolean_array_slicing() {
        // Slicing starting at offset 1, length 3: [None, Some(false), None]
        let original_vec = vec![Some(true), None, Some(false), None, Some(true)];
        let array = BooleanArray::from(original_vec);

        let sliced_array_ref = array.slice(1, 3);

        // Assert length and nulls
        assert_eq!(sliced_array_ref.len(), 3);
        assert!(sliced_array_ref.nulls().is_some());
        assert_eq!(sliced_array_ref.nulls().unwrap().null_count(), 2); // 2 nulls in this slice!

        // Slice concrete verification
        let sliced_array = BooleanArray {
            values: array.values.slice(1, 3),
            nulls: array.nulls.as_ref().map(|n| n.slice(1, 3)),
        };

        assert_eq!(sliced_array.len(), 3);
        assert!(sliced_array.is_null(0)); // Original index 1 is now sliced 0 (Null)
        assert!(!sliced_array.value(0));
        assert!(!sliced_array.is_null(1)); // Original index 2 is now sliced 1 (Valid false)
        assert!(!sliced_array.value(1));
    }

    #[test]
    fn test_boolean_array_take_and_iter() {
        // 1. Create source array: [true, None, false, true, None]
        let original = BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None]);

        // 2. Iterate and verify original values
        let gathered: Vec<Option<bool>> = original.iter().collect();
        assert_eq!(
            gathered,
            vec![Some(true), None, Some(false), Some(true), None]
        );

        // 3. Create indices array: [2, 0, null, 3, 100]
        let indices = PrimitiveArray::from(vec![Some(2i32), Some(0), None, Some(3), Some(100)]);

        // 4. Perform take!
        let taken = original.take(&indices).unwrap();

        assert_eq!(taken.len(), 5);
        let taken_gathered: Vec<Option<bool>> = taken.iter().collect();

        // Index 2 -> Some(false)
        // Index 0 -> Some(true)
        // Index null -> None
        // Index 3 -> Some(true)
        // Index 100 (out of bounds) -> None
        assert_eq!(
            taken_gathered,
            vec![Some(false), Some(true), None, Some(true), None]
        );
    }

    #[test]
    fn test_boolean_array_bitwise_ops() {
        // LHS: [Some(true), None, Some(false), Some(true)]
        let lhs = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        // RHS: [Some(false), Some(true), None, Some(true)]
        let rhs = BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);

        // 1. Bitwise AND: result values should be AND'ed, and nulls propagated
        // Expected: [Some(false), None, None, Some(true)]
        let result_and = lhs.bitwise_bin_op(&rhs, |a, b| a & b);
        let gathered_and: Vec<Option<bool>> = result_and.iter().collect();
        assert_eq!(gathered_and, vec![Some(false), None, None, Some(true)]);

        // 2. Bitwise OR: result values should be OR'ed, and nulls propagated
        // Expected: [Some(true), None, None, Some(true)]
        let result_or = lhs.bitwise_bin_op(&rhs, |a, b| a | b);
        let gathered_or: Vec<Option<bool>> = result_or.iter().collect();
        assert_eq!(gathered_or, vec![Some(true), None, None, Some(true)]);
    }
}

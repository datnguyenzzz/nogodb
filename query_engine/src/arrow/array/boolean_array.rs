use std::sync::Arc;

use crate::arrow::{Array, BooleanBuffer, Buffer, DataType, NullBuffer};

pub struct BooleanArray {
    values: BooleanBuffer,
    nulls: Option<NullBuffer>,
}

impl Array for BooleanArray {
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
}

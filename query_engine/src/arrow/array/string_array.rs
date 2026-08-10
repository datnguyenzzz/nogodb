// https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout

use std::{ptr, sync::Arc};

use crate::arrow::{Array, ArrayRef, BooleanBuffer, Buffer, DataType, NullBuffer};

pub struct StringArray {
    offsets_value: Buffer, // each offset value is i32
    data_value: Buffer,
    nulls: Option<NullBuffer>,
}

impl Array for StringArray {
    fn len(&self) -> usize {
        self.offsets_value.len() / 4 - 1
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let offset_bytes = offset * 4;
        let length_bytes = (length + 1) * 4;
        Arc::new(Self {
            offsets_value: self.offsets_value.slice(offset_bytes, length_bytes),
            // we keep the data_buffer untouched because offsets still index into it
            data_value: self.data_value.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
        })
    }

    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
}

impl StringArray {
    pub fn new(offsets_value: Buffer, data_value: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self {
            offsets_value,
            data_value,
            nulls,
        }
    }

    /// Returns the primitive value at index `index` with zero-copy casting
    pub fn value(&self, index: usize) -> &str {
        assert!(index < self.len());

        let (start, end) = unsafe {
            (
                self.offsets_value.get_uncheck::<i32>(index) as usize,
                self.offsets_value.get_uncheck::<i32>(index + 1) as usize,
            )
        };

        let bytes = &self.data_value.as_slice()[start..end];
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

impl From<Vec<&str>> for StringArray {
    fn from(value: Vec<&str>) -> Self {
        let len = value.len();
        let mut offsets = Vec::with_capacity(len + 1);
        let mut raw_byte = Vec::new();
        let mut offset = 0i32;
        offsets.push(offset);
        for val in value {
            raw_byte.extend_from_slice(val.as_bytes());
            offset += val.len() as i32;
            offsets.push(offset)
        }

        let offset_len = offsets.len().saturating_mul(4);
        let mut offset_bytes = Vec::with_capacity(offset_len);
        unsafe {
            ptr::copy_nonoverlapping(
                offsets.as_ptr() as *const u8,
                offset_bytes.as_mut_ptr(),
                offset_len,
            );
            offset_bytes.set_len(offset_len);
        }

        Self::new(Buffer::from(offset_bytes), Buffer::from(raw_byte), None)
    }
}

impl From<Vec<String>> for StringArray {
    fn from(value: Vec<String>) -> Self {
        let refs: Vec<&str> = value.iter().map(|s| s.as_str()).collect();
        refs.into()
    }
}

impl From<Vec<Option<&str>>> for StringArray {
    fn from(value: Vec<Option<&str>>) -> Self {
        let len = value.len();
        let mut offsets = Vec::with_capacity(len + 1);
        let mut raw_byte = Vec::new();
        let mut validity = vec![0u8; (len + 7) / 8];
        let mut offset = 0i32;
        offsets.push(offset);

        for (i, opt) in value.into_iter().enumerate() {
            if let Some(val) = opt {
                raw_byte.extend_from_slice(val.as_bytes());
                offset += val.len() as i32;
                validity[i / 8] |= 1 << (i % 8);
            }

            offsets.push(offset)
        }

        let offset_len = offsets.len().saturating_mul(4);
        let mut offset_bytes = Vec::with_capacity(offset_len);
        unsafe {
            ptr::copy_nonoverlapping(
                offsets.as_ptr() as *const u8,
                offset_bytes.as_mut_ptr(),
                offset_len,
            );
            offset_bytes.set_len(offset_len);
        }

        Self::new(
            Buffer::from(offset_bytes),
            Buffer::from(raw_byte),
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity),
                0,
                len,
            ))),
        )
    }
}

impl From<Vec<Option<String>>> for StringArray {
    fn from(value: Vec<Option<String>>) -> Self {
        let refs: Vec<Option<&str>> = value.iter().map(|s| s.as_deref()).collect();
        refs.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_array_un_nullable() {
        // Convert from Vec<&str>
        let original_vec = vec!["Alice", "Bob", "Charlie"];
        let array = StringArray::from(original_vec);

        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Utf8);
        assert!(array.nulls().is_none());

        assert_eq!(array.value(0), "Alice");
        assert_eq!(array.value(1), "Bob");
        assert_eq!(array.value(2), "Charlie");

        // Convert from Vec<String>
        let strings_vec = vec!["X".to_string(), "Y".to_string()];
        let str_array = StringArray::from(strings_vec);
        assert_eq!(str_array.len(), 2);
        assert_eq!(str_array.value(0), "X");
        assert_eq!(str_array.value(1), "Y");
    }

    #[test]
    fn test_string_array_nullable() {
        // Convert from Vec<Option<&str>>
        let original_vec = vec![Some("Alice"), None, Some("Bob"), None, Some("Charlie")];
        let array = StringArray::from(original_vec);

        assert_eq!(array.len(), 5);
        assert!(array.nulls().is_some());

        let null_buf = array.nulls().unwrap();
        assert_eq!(null_buf.null_count(), 2); // Eagerly computed 2 nulls!

        assert_eq!(array.value(0), "Alice");
        assert!(array.is_null(1));
        assert_eq!(array.value(1), ""); // Null string yields empty
        assert_eq!(array.value(2), "Bob");
        assert!(array.is_null(3));
        assert_eq!(array.value(4), "Charlie");

        // Convert from Vec<Option<String>>
        let strings_vec = vec![Some("Hello".to_string()), None];
        let str_array = StringArray::from(strings_vec);
        assert_eq!(str_array.len(), 2);
        assert_eq!(str_array.value(0), "Hello");
        assert!(str_array.is_null(1));
    }

    #[test]
    fn test_string_array_slicing() {
        // Slicing starting at offset 2, length 2: [Some("Bob"), Some("Charlie")]
        let original_vec = vec![Some("Alice"), None, Some("Bob"), Some("Charlie"), None];
        let array = StringArray::from(original_vec);

        let sliced_array_ref = array.slice(2, 2);

        // Assert length and nulls
        assert_eq!(sliced_array_ref.len(), 2);
        assert!(sliced_array_ref.nulls().is_some());
        assert_eq!(sliced_array_ref.nulls().unwrap().null_count(), 0); // 0 nulls in this slice!

        // Slice concrete verification
        let sliced_array = StringArray {
            offsets_value: array.offsets_value.slice(2 * 4, 3 * 4), // offset 2 starts at index 2, length 2 (3 offsets)
            data_value: array.data_value.clone(),
            nulls: array.nulls.as_ref().map(|n| n.slice(2, 2)),
        };

        assert_eq!(sliced_array.len(), 2);
        assert_eq!(sliced_array.value(0), "Bob");
        assert_eq!(sliced_array.value(1), "Charlie");
    }
}

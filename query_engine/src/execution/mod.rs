use core::slice;
use std::cmp::{self, Ordering};

pub mod aggregate;
pub mod dispatcher;
pub mod filters;
pub mod hash_join;
pub mod limit;
pub mod numa_topology;
pub mod physical_plan;
pub mod pipeline;
pub mod scan;
pub mod scheduler;
pub mod sort;

/// Represents a NUMA-aware dynamic execution block of roughly [`MORSEL_SIZE`] rows.
#[derive(Clone, Copy)]
pub struct Morsel {
    pub start_row: usize,
    pub num_rows: usize,
    pub numa_node: usize,
}

/// A 16-byte German String
/// - If len <= 12: Inlined string bytes stored entirely within the 16-byte struct.
/// - If len > 12: 4-byte prefix + 8-byte pointer (or coordinate) to the string buffer.
/// Reference: https://cedardb.com/blog/german_strings/
#[derive(Clone, Copy)]
pub struct GermanString {
    pub length: u32,
    pub prefix: [u8; 4],
    pub trailing: Trailing,
}

#[derive(Clone, Copy)]
pub union Trailing {
    pub buf: [u8; 8],
    pub ptr: *const u8,
}

unsafe impl Send for GermanString {}
unsafe impl Sync for GermanString {}

impl GermanString {
    pub fn from_str(s: &str) -> Self {
        let bytes = s.as_bytes();
        let length = bytes.len() as u32;
        let mut prefix = [0u8; 4];

        if length <= 12 {
            let mut buf = [0u8; 8];
            let prefix_len = cmp::min(bytes.len(), 4);
            prefix[..prefix_len].copy_from_slice(&bytes[..prefix_len]);

            if length > 4 {
                let rest_len = (length - 4) as usize;
                buf[..rest_len].copy_from_slice(&bytes[4..4 + rest_len]);
            }

            Self {
                length,
                prefix,
                trailing: Trailing { buf },
            }
        } else {
            prefix.copy_from_slice(&bytes[..4]);

            Self {
                length,
                prefix,
                trailing: Trailing {
                    ptr: bytes.as_ptr(),
                },
            }
        }
    }

    pub fn as_str(&self) -> &str {
        if self.length <= 12 {
            unsafe {
                let ptr = &self.prefix as *const u8;
                str::from_utf8_unchecked(slice::from_raw_parts(ptr, self.length as usize))
            }
        } else {
            unsafe {
                str::from_utf8_unchecked(slice::from_raw_parts(
                    self.trailing.ptr,
                    self.length as usize,
                ))
            }
        }
    }
}

impl PartialOrd for GermanString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GermanString {
    fn cmp(&self, other: &Self) -> Ordering {
        let p1 = u32::from_be_bytes(self.prefix);
        let p2 = u32::from_be_bytes(other.prefix);
        let prefix_ord = p1.cmp(&p2);
        if prefix_ord != Ordering::Equal {
            return prefix_ord;
        }

        if self.length <= 4 && other.length <= 4 {
            return self.length.cmp(&other.length);
        }

        if self.length <= 12 && other.length <= 12 {
            let t1 = (self.length - 4) as usize;
            let t2 = (other.length - 4) as usize;
            unsafe {
                let s1 = &self.trailing.buf[..t1];
                let s2 = &other.trailing.buf[..t2];
                let ord = s1.cmp(&s2);
                if ord != Ordering::Equal {
                    return ord;
                }
                return self.length.cmp(&other.length);
            }
        }

        unsafe {
            let s1 = if self.length <= 12 {
                &self.trailing.buf[..(self.length - 4) as usize]
            } else {
                slice::from_raw_parts(self.trailing.ptr.add(4), (self.length - 4) as usize)
            };

            let s2 = if other.length <= 12 {
                &other.trailing.buf[..(other.length - 4) as usize]
            } else {
                slice::from_raw_parts(other.trailing.ptr.add(4), (other.length - 4) as usize)
            };

            let ord = s1.cmp(&s2);
            if ord != Ordering::Equal {
                return ord;
            }
            return self.length.cmp(&other.length);
        }
    }
}

impl PartialEq for GermanString {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for GermanString {}

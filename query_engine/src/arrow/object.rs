use serde::{Deserialize, Serialize};

use crate::arrow::{
    Array, DataType,
    aggregation::{self, max_str, min_str},
    array::{BooleanArray, PrimitiveArray, StringArray},
};

pub const MAX_STAT_STRING_BYTES: usize = 32;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ColumnStat {
    Int64 { min: i64, max: i64 },
    Float64 { min: f64, max: f64 },
    Utf8 { min: String, max: String },
    Bool { has_true: bool, has_false: bool },
}

impl ColumnStat {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ColumnStat::Int64 { min, max } => {
                let mut bytes = Vec::with_capacity(17);
                bytes.push(0);
                bytes.extend_from_slice(&min.to_le_bytes());
                bytes.extend_from_slice(&max.to_le_bytes());
                bytes
            }
            ColumnStat::Float64 { min, max } => {
                let mut bytes = Vec::with_capacity(17);
                bytes.push(1);
                bytes.extend_from_slice(&min.to_bits().to_le_bytes());
                bytes.extend_from_slice(&max.to_bits().to_le_bytes());
                bytes
            }
            ColumnStat::Utf8 { min, max } => {
                let mut bytes = Vec::with_capacity(69);
                bytes.push(2);
                let min_b = min.as_bytes();
                let min_len = min_b.len().min(MAX_STAT_STRING_BYTES);
                bytes.extend_from_slice(&(min_len as u16).to_le_bytes());
                bytes.extend_from_slice(&min_b[..min_len]);

                // truncate and increament for max value, to avoid incorrectly discard chunks
                let max_b = max.as_bytes();
                let ptr = max_b.as_ptr() as *mut u8;
                let max_len = max_b.len().min(MAX_STAT_STRING_BYTES);
                bytes.extend_from_slice(&(max_len as u16).to_le_bytes());
                for i in (0..max_len).rev() {
                    if max_b[i] != 0xff {
                        unsafe { *ptr.add(i) = max_b[i] + 1 }
                        break;
                    }
                    unsafe { *ptr.add(i) = 0 }
                }
                bytes.extend_from_slice(&max_b[..max_len]);
                bytes
            }
            ColumnStat::Bool {
                has_true,
                has_false,
            } => {
                let mut bytes = Vec::with_capacity(3);
                bytes.push(3);
                bytes.push(if *has_false { 1 } else { 0 });
                bytes.push(if *has_true { 1 } else { 0 });
                bytes
            }
        }
    }

    pub fn decode(slice: &[u8]) -> Option<Self> {
        if slice.is_empty() {
            return None;
        }

        match slice[0] {
            0 => {
                // Int64
                if slice.len() != 17 {
                    return None;
                }
                let min = i64::from_le_bytes(slice[1..9].try_into().ok()?);
                let max = i64::from_le_bytes(slice[9..17].try_into().ok()?);
                Some(ColumnStat::Int64 { min, max })
            }
            1 => {
                // Float64
                if slice.len() != 17 {
                    return None;
                }
                let min = f64::from_bits(u64::from_le_bytes(slice[1..9].try_into().ok()?));
                let max = f64::from_bits(u64::from_le_bytes(slice[9..17].try_into().ok()?));
                Some(ColumnStat::Float64 { min, max })
            }
            2 => {
                // utf8
                if slice.len() < 3 {
                    return None;
                }
                let min_len = u16::from_le_bytes(slice[1..3].try_into().ok()?) as usize;
                let min_end = 3 + min_len;
                if slice.len() < min_end + 2 {
                    return None;
                }
                let min = str::from_utf8(&slice[3..min_end]).ok()?.to_string();

                let max_len =
                    u16::from_le_bytes(slice[min_end..min_end + 2].try_into().ok()?) as usize;
                let max_end = min_end + 2 + max_len;
                if slice.len() < max_end {
                    return None;
                }
                let max = str::from_utf8(&slice[min_end + 2..max_end])
                    .ok()?
                    .to_string();

                Some(ColumnStat::Utf8 { min, max })
            }
            3 => {
                // bool
                if slice.len() < 3 {
                    return None;
                }
                let has_false = slice[1] != 0;
                let has_true = slice[2] != 0;
                Some(ColumnStat::Bool {
                    has_true,
                    has_false,
                })
            }
            _ => None,
        }
    }

    pub fn is_disjoint(&self, other: &ColumnStat) -> bool {
        match (self, other) {
            (
                ColumnStat::Int64 {
                    min: min_a,
                    max: max_a,
                },
                ColumnStat::Int64 {
                    min: min_b,
                    max: max_b,
                },
            ) => max_a < min_b || min_a > max_b,
            (
                ColumnStat::Float64 {
                    min: min_a,
                    max: max_a,
                },
                ColumnStat::Float64 {
                    min: min_b,
                    max: max_b,
                },
            ) => max_a < min_b || min_a > max_b,
            (
                ColumnStat::Utf8 {
                    min: min_a,
                    max: max_a,
                },
                ColumnStat::Utf8 {
                    min: min_b,
                    max: max_b,
                },
            ) => max_a < min_b || min_a > max_b,
            (
                ColumnStat::Bool {
                    has_true: t_a,
                    has_false: f_a,
                },
                ColumnStat::Bool {
                    has_true: t_b,
                    has_false: f_b,
                },
            ) => {
                // Disjoint if one set is exclusively true and the other is exclusively false
                (*t_a && !*f_a && !*t_b && *f_b) || (!*t_a && *f_a && *t_b && !*f_b)
            }
            _ => false,
        }
    }
}

pub fn compute_column_stat(array: &dyn Array) -> Option<ColumnStat> {
    if array.len() == 0
        || array
            .nulls()
            .map(|n| n.null_count() == array.len())
            .unwrap_or(false)
    {
        return None;
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<i8>>()?;
            Some(ColumnStat::Int64 {
                min: aggregation::min(arr)? as i64,
                max: aggregation::max(arr)? as i64,
            })
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<i16>>()?;
            Some(ColumnStat::Int64 {
                min: aggregation::min(arr)? as i64,
                max: aggregation::max(arr)? as i64,
            })
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<i32>>()?;
            Some(ColumnStat::Int64 {
                min: aggregation::min(arr)? as i64,
                max: aggregation::max(arr)? as i64,
            })
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<i64>>()?;
            Some(ColumnStat::Int64 {
                min: aggregation::min(arr)?,
                max: aggregation::max(arr)?,
            })
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<f64>>()?;
            Some(ColumnStat::Float64 {
                min: aggregation::min_float(arr)?,
                max: aggregation::max_float(arr)?,
            })
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(ColumnStat::Utf8 {
                min: min_str(arr)?.to_string(),
                max: max_str(arr)?.to_string(),
            })
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            let has_false = aggregation::min_boolean(arr).map(|v| !v).unwrap_or(false);
            let has_true = aggregation::max_boolean(arr).map(|v| v).unwrap_or(false);
            Some(ColumnStat::Bool {
                has_true,
                has_false,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_stat_int64_roundtrip_and_disjoint() {
        let stat = ColumnStat::Int64 {
            min: -100,
            max: 200,
        };
        let encoded = stat.encode();
        let decoded = ColumnStat::decode(&encoded).expect("Failed to decode Int64 stat");
        assert_eq!(stat, decoded);

        let non_overlapping = ColumnStat::Int64 { min: 201, max: 500 };
        assert!(stat.is_disjoint(&non_overlapping));

        let overlapping = ColumnStat::Int64 { min: 150, max: 300 };
        assert!(!stat.is_disjoint(&overlapping));
    }

    #[test]
    fn test_column_stat_float64_roundtrip_and_disjoint() {
        let stat = ColumnStat::Float64 {
            min: -15.5,
            max: 99.75,
        };
        let encoded = stat.encode();
        let decoded = ColumnStat::decode(&encoded).expect("Failed to decode Float64 stat");
        assert_eq!(stat, decoded);

        let disjoint = ColumnStat::Float64 {
            min: 100.0,
            max: 200.0,
        };
        assert!(stat.is_disjoint(&disjoint));
    }

    #[test]
    fn test_column_stat_utf8_capping_and_disjoint() {
        let long_str = "a".repeat(100);
        let stat = ColumnStat::Utf8 {
            min: long_str.clone(),
            max: "zzzz".to_string(),
        };
        let encoded = stat.encode();
        let decoded = ColumnStat::decode(&encoded).expect("Failed to decode Utf8 stat");
        match decoded {
            ColumnStat::Utf8 { min, max } => {
                assert_eq!(min.len(), MAX_STAT_STRING_BYTES);
                // max was incremented on the last byte from 'z' (0x7A) to '{' (0x7B)
                assert_eq!(max, "zzz{");
            }
            _ => panic!("Expected Utf8 stat"),
        }
    }

    #[test]
    fn test_column_stat_utf8_max_truncation_and_increment() {
        // A long string exceeding 32 bytes:
        // Truncated alone would be "hello_world_this_is_a_very_long"
        // With increment, it becomes "hello_world_this_is_a_very_lonh"
        // which guarantees that any probe matching "hello_world_this_is_a_very_long_xyz" is NOT discarded!
        let min_str = "a".to_string();
        let original_max = "hello_world_this_is_a_very_long_string_12345".to_string();

        let stat = ColumnStat::Utf8 {
            min: min_str,
            max: original_max.clone(),
        };
        let encoded = stat.encode();
        let decoded = ColumnStat::decode(&encoded).expect("Failed to decode Utf8 stat");

        match decoded {
            ColumnStat::Utf8 { min, max } => {
                assert_eq!(min, "a");
                assert_eq!(max.len(), MAX_STAT_STRING_BYTES);
                let raw_prefix = &original_max[..MAX_STAT_STRING_BYTES];
                assert!(
                    max.as_str() > raw_prefix,
                    "Truncated max must be incremented to avoid false-positive pruning"
                );

                // Verify that the probe with the original long max string is correctly NOT disjoint!
                let probe = ColumnStat::Utf8 {
                    min: original_max.clone(),
                    max: original_max.clone(),
                };
                assert!(
                    !stat.is_disjoint(&probe),
                    "Original item must not be pruned by truncated/incremented bound"
                );
            }
            _ => panic!("Expected Utf8 stat"),
        }
    }

    #[test]
    fn test_column_stat_bool_disjoint() {
        let true_only = ColumnStat::Bool {
            has_true: true,
            has_false: false,
        };
        let false_only = ColumnStat::Bool {
            has_true: false,
            has_false: true,
        };
        let both = ColumnStat::Bool {
            has_true: true,
            has_false: true,
        };

        assert!(true_only.is_disjoint(&false_only));
        assert!(!true_only.is_disjoint(&both));
    }
}

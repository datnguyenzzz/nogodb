use std::cmp::Ordering;

use anyhow::{Result, anyhow};

use crate::arrow::{
    Array, DataType, NullBuffer, Scalar,
    array::{BooleanArray, PrimitiveArray, StringArray},
};

pub enum Op {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    // Not yet supported
    // Distinct,
    // NotDistinct,
}

/// Options that define the sort order of a given column
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            // default to nulls first to match spark's behavior
            nulls_first: true,
        }
    }
}

/// Compare values at arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

fn compare<A, F>(l: &A, r: &A, opt: SortOptions, cmp: F) -> DynComparator
where
    A: Array + Clone,
    F: Fn(usize, usize) -> Ordering + Send + Sync + 'static,
{
    let l = l.logical_nulls().filter(|x| x.null_count() > 0);
    let r = r.logical_nulls().filter(|x| x.null_count() > 0);
    match (opt.nulls_first, opt.descending) {
        (true, true) => compare_impl::<true, true, _>(l, r, cmp),
        (true, false) => compare_impl::<true, false, _>(l, r, cmp),
        (false, true) => compare_impl::<false, true, _>(l, r, cmp),
        (false, false) => compare_impl::<false, false, _>(l, r, cmp),
    }
}

fn compare_impl<const NULLS_FIRST: bool, const DESCENDING: bool, F>(
    l: Option<NullBuffer>,
    r: Option<NullBuffer>,
    cmp: F,
) -> DynComparator
where
    F: Fn(usize, usize) -> Ordering + Send + Sync + 'static,
{
    let cmp = move |i, j| match DESCENDING {
        true => cmp(i, j).reverse(),
        false => cmp(i, j),
    };

    let (left_null, right_null) = match NULLS_FIRST {
        true => (Ordering::Less, Ordering::Greater),
        false => (Ordering::Greater, Ordering::Less),
    };

    match (l, r) {
        (None, None) => Box::new(cmp),
        (Some(l), None) => Box::new(move |i, j| match l.is_null(i) {
            true => left_null,
            false => cmp(i, j),
        }),
        (None, Some(r)) => Box::new(move |i, j| match r.is_null(j) {
            true => right_null,
            false => cmp(i, j),
        }),
        (Some(l), Some(r)) => Box::new(move |i, j| match (l.is_null(i), r.is_null(j)) {
            (true, true) => Ordering::Equal,
            (true, false) => left_null,
            (false, true) => right_null,
            (false, false) => cmp(i, j),
        }),
    }
}

pub fn build_compare(l: &dyn Array, r: &dyn Array, opt: SortOptions) -> Result<DynComparator> {
    if l.data_type() != r.data_type() {
        return Err(anyhow!(
            "Cannot compare mismatched types: {:?} vs {:?}",
            l.data_type(),
            r.data_type()
        ));
    }

    match l.data_type() {
        DataType::Int8 => {
            let l_arr = l
                .as_any()
                .downcast_ref::<PrimitiveArray<i8>>()
                .unwrap()
                .clone();
            let r_arr = r
                .as_any()
                .downcast_ref::<PrimitiveArray<i8>>()
                .unwrap()
                .clone();
            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned.value(i).cmp(&r_arr_cloned.value(j))
            }))
        }
        DataType::Int16 => {
            let l_arr = l
                .as_any()
                .downcast_ref::<PrimitiveArray<i16>>()
                .unwrap()
                .clone();
            let r_arr = r
                .as_any()
                .downcast_ref::<PrimitiveArray<i16>>()
                .unwrap()
                .clone();

            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned.value(i).cmp(&r_arr_cloned.value(j))
            }))
        }
        DataType::Int32 => {
            let l_arr = l
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap()
                .clone();
            let r_arr = r
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap()
                .clone();
            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned.value(i).cmp(&r_arr_cloned.value(j))
            }))
        }
        DataType::Int64 => {
            let l_arr = l
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .clone();
            let r_arr = r
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .clone();

            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned.value(i).cmp(&r_arr_cloned.value(j))
            }))
        }
        DataType::Float64 => {
            let l_arr = l
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap()
                .clone();
            let r_arr = r
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap()
                .clone();

            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned
                    .value(i)
                    .partial_cmp(&r_arr_cloned.value(j))
                    .unwrap_or(Ordering::Equal)
            }))
        }
        DataType::Utf8 => {
            let l_arr = l.as_any().downcast_ref::<StringArray>().unwrap().clone();
            let r_arr = r.as_any().downcast_ref::<StringArray>().unwrap().clone();
            let l_arr_cloned = l_arr.clone();
            let r_arr_cloned = r_arr.clone();

            Ok(compare(&l_arr, &r_arr, opt, move |i, j| {
                l_arr_cloned.value(i).cmp(r_arr_cloned.value(j))
            }))
        }
        DataType::Boolean => {
            let l_arr = l.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            let r_arr = r.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            let result_arr = l_arr.bitwise_bin_op(&r_arr, |a, b| !(a ^ b));
            let result_arr_cloned = result_arr.clone();
            Ok(Box::new(move |i, _j| {
                if result_arr_cloned.value(i) {
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            }))
        }
        other => Err(anyhow!(
            "Compare builder not yet implemented for DataType: {:?}",
            other
        )),
    }
}

/// A possibly [`Scalar`] [`Array`]
pub trait Datum {
    /// Returns the value for this [`Datum`] and a boolean indicating if the value is scalar
    fn get(&self) -> (&dyn Array, bool);
}

impl<T: Array> Datum for T {
    fn get(&self) -> (&dyn Array, bool) {
        (self, false)
    }
}

impl Datum for dyn Array {
    fn get(&self) -> (&dyn Array, bool) {
        (self, false)
    }
}

impl Datum for &dyn Array {
    fn get(&self) -> (&dyn Array, bool) {
        (*self, false)
    }
}

impl Datum for Scalar {
    fn get(&self) -> (&dyn Array, bool) {
        (self.0.as_ref(), true)
    }
}

/// Perform `left == right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::Equal, lhs, rhs)
}

/// Perform `left != right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::NotEqual, lhs, rhs)
}

/// Perform `left < right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::Less, lhs, rhs)
}

/// Perform `left <= right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::LessEqual, lhs, rhs)
}

/// Perform `left > right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::Greater, lhs, rhs)
}

/// Perform `left >= right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    compare_op(Op::GreaterEqual, lhs, rhs)
}

fn compare_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray> {
    let (l, l_s) = lhs.get();
    let (r, r_s) = rhs.get();
    let l_len = l.len();
    let r_len = r.len();

    // Symmetrically enforce length bounds only if neither is a scalar!
    if !l_s && !r_s && l_len != r_len {
        return Err(anyhow!(
            "Cannot compare arrays of different lengths, got {l_len} vs {r_len}"
        ));
    }

    let len = match l_s {
        true => r_len,
        false => l_len,
    };

    let new_nulls = match (l.nulls(), r.nulls()) {
        (Some(n1), Some(n2)) => Some(NullBuffer::new(
            n1.inner().bitwise_bin_op(n2.inner(), |a, b| a & b),
        )),
        (Some(n1), None) => {
            if l_s {
                None
            } else {
                Some(n1.clone())
            }
        }
        (None, Some(n2)) => {
            if r_s {
                None
            } else {
                Some(n2.clone())
            }
        }
        (None, None) => None,
    };

    let cmp = build_compare(l, r, SortOptions::default())?;
    let res: BooleanArray = (0..len)
        .map(|idx| {
            let is_null = new_nulls.as_ref().map_or(false, |n| n.is_null(idx));
            if is_null {
                false
            } else {
                let l_idx = if l_s { 0 } else { idx };
                let r_idx = if r_s { 0 } else { idx };

                let ord = cmp(l_idx, r_idx);
                match op {
                    Op::Equal => ord == Ordering::Equal,
                    Op::NotEqual => ord != Ordering::Equal,
                    Op::Less => ord == Ordering::Less,
                    Op::LessEqual => ord != Ordering::Greater,
                    Op::Greater => ord == Ordering::Greater,
                    Op::GreaterEqual => ord != Ordering::Less,
                }
            }
        })
        .collect();

    let (values, _) = res.into_parts();
    Ok(BooleanArray::new(values, new_nulls))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_ord_build_compare_primitive() {
        let left = PrimitiveArray::from(vec![Some(10i32), None, Some(30)]);
        let right = PrimitiveArray::from(vec![Some(20i32), Some(20), Some(20)]);

        // 1. Ascending, Nulls First (Default)
        // - Row 0: 10 vs 20 -> Less
        // - Row 1: Null vs 20 -> Less (since Nulls First)
        // - Row 2: 30 vs 20 -> Greater
        let cmp = build_compare(&left, &right, SortOptions::default()).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Less);
        assert_eq!(cmp(1, 1), Ordering::Less);
        assert_eq!(cmp(2, 2), Ordering::Greater);

        // 2. Descending, Nulls Last
        let opts = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let cmp_desc = build_compare(&left, &right, opts).unwrap();
        assert_eq!(cmp_desc(0, 0), Ordering::Greater); // 10 < 20 reversed -> Greater
        assert_eq!(cmp_desc(1, 1), Ordering::Greater); // Null is last -> Greater
        assert_eq!(cmp_desc(2, 2), Ordering::Less); // 30 > 20 reversed -> Less
    }

    #[test]
    fn test_high_level_comparisons() {
        let left = PrimitiveArray::from(vec![Some(10i32), None, Some(30)]);
        let right = PrimitiveArray::from(vec![Some(15i32), Some(20), Some(20)]);

        // 1. Array vs Array Less Than (lt)
        // Expected: [Some(true), None, Some(false)] (null is propagated!)
        let res_lt = lt(&left, &right).unwrap();
        let gathered: Vec<Option<bool>> = res_lt.iter().collect();
        assert_eq!(gathered, vec![Some(true), None, Some(false)]);

        // 2. Array vs Scalar Greater Than (gt)
        // Compare left > 15 (Scalar)
        let scalar_val = PrimitiveArray::from(vec![15i32]);
        let scalar = Scalar::new(Arc::new(scalar_val));

        // Expected: [Some(false), None, Some(true)]
        let res_gt = gt(&left, &scalar).unwrap();
        let gathered_gt: Vec<Option<bool>> = res_gt.iter().collect();
        assert_eq!(gathered_gt, vec![Some(false), None, Some(true)]);
    }

    #[test]
    fn test_ord_comparisons_float64() {
        let left = PrimitiveArray::from(vec![Some(10.5f64), None, Some(30.0)]);
        let right = PrimitiveArray::from(vec![Some(15.0f64), Some(20.0), Some(20.0)]);

        // 1. Array vs Array Less Than (lt)
        // Expected: [Some(true), None, Some(false)]
        let res_lt = lt(&left, &right).unwrap();
        let gathered: Vec<Option<bool>> = res_lt.iter().collect();
        assert_eq!(gathered, vec![Some(true), None, Some(false)]);

        // 2. Array vs Scalar Less Than-or-Equal (lt_eq)
        // Compare left <= 15.0
        let scalar_val = PrimitiveArray::from(vec![15.0f64]);
        let scalar = Scalar::new(Arc::new(scalar_val));

        // Expected: [Some(true), None, Some(false)]
        let res_lt_eq = lt_eq(&left, &scalar).unwrap();
        let gathered_lt_eq: Vec<Option<bool>> = res_lt_eq.iter().collect();
        assert_eq!(gathered_lt_eq, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn test_ord_comparisons_utf8() {
        let left = StringArray::from(vec![Some("Alice"), None, Some("Charlie")]);
        let right = StringArray::from(vec![Some("Bob"), Some("Bob"), Some("Charlie")]);

        // 1. Array vs Array Equality (eq)
        // Expected: [Some(false), None, Some(true)]
        let res_eq = eq(&left, &right).unwrap();
        let gathered: Vec<Option<bool>> = res_eq.iter().collect();
        assert_eq!(gathered, vec![Some(false), None, Some(true)]);
    }

    #[test]
    fn test_ord_comparisons_boolean() {
        let left = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let right = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);

        // 1. Array vs Array Equality (eq)
        // Expected: [Some(true), None, Some(true)]
        let res_eq = eq(&left, &right).unwrap();
        let gathered: Vec<Option<bool>> = res_eq.iter().collect();
        assert_eq!(gathered, vec![Some(true), None, Some(true)]);
    }
}

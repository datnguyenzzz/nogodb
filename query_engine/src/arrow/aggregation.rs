use crate::arrow::array::{BooleanArray, NativeType, PrimitiveArray, StringArray};

pub fn min<T: NativeType + Ord>(array: &PrimitiveArray<T>) -> Option<T> {
    array.iter().flatten().min()
}

pub fn max<T: NativeType + Ord>(array: &PrimitiveArray<T>) -> Option<T> {
    array.iter().flatten().max()
}

pub fn min_float(array: &PrimitiveArray<f64>) -> Option<f64> {
    array
        .iter()
        .flatten()
        .filter(|x| !x.is_nan())
        .min_by(|a, b| a.partial_cmp(b).unwrap())
}

pub fn max_float(array: &PrimitiveArray<f64>) -> Option<f64> {
    array
        .iter()
        .flatten()
        .filter(|x| !x.is_nan())
        .max_by(|a, b| a.partial_cmp(b).unwrap())
}

pub fn min_boolean(array: &BooleanArray) -> Option<bool> {
    let mut found_any = false;
    for val in array.iter().flatten() {
        if !val {
            return Some(false);
        }
        found_any = true;
    }
    if found_any { Some(true) } else { None }
}

pub fn max_boolean(array: &BooleanArray) -> Option<bool> {
    let mut found_any = false;
    for val in array.iter().flatten() {
        if val {
            return Some(true); // true is the absolute maximum boolean
        }
        found_any = true;
    }
    if found_any { Some(false) } else { None }
}

pub fn min_str<'a>(array: &'a StringArray) -> Option<&'a str> {
    array.iter().flatten().min()
}

pub fn max_str<'a>(array: &'a StringArray) -> Option<&'a str> {
    array.iter().flatten().max()
}

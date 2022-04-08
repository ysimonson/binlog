use super::Error;
use std::cmp::Ordering;
use std::ops::Bound;

fn unwrap_bound(bound: Bound<&i64>) -> Option<i64> {
    match bound {
        Bound::Included(ts) => Some(*ts),
        Bound::Excluded(ts) => Some(*ts),
        _ => None,
    }
}

pub(crate) fn check_bounds(start_bound: Bound<&i64>, end_bound: Bound<&i64>) -> Result<(), Error> {
    if let (Some(start_ts), Some(end_ts)) = (unwrap_bound(start_bound), unwrap_bound(end_bound)) {
        match start_ts.cmp(&end_ts) {
            Ordering::Less => {}
            Ordering::Equal => {
                if matches!(start_bound, Bound::Excluded(_)) || matches!(end_bound, Bound::Excluded(_)) {
                    return Err(Error::BadRange);
                }
            }
            Ordering::Greater => return Err(Error::BadRange),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    #[test]
    fn check_bounds() {
        super::check_bounds(Bound::Unbounded, Bound::Unbounded).unwrap();
        super::check_bounds(Bound::Included(&0), Bound::Unbounded).unwrap();
        // TODO: maybe this should return a BadRange?
        super::check_bounds(Bound::Unbounded, Bound::Included(&0)).unwrap();
        super::check_bounds(Bound::Unbounded, Bound::Excluded(&0)).unwrap();
    }
}

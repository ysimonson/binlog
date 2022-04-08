use std::ops::Bound;
use super::Error;

fn unwrap_bound(bound: Bound<&i64>) -> Option<i64> {
    match bound {
        Bound::Included(ts) => Some(*ts),
        Bound::Excluded(ts) => Some(*ts),
        _ => None
    }
}

pub(crate) fn check_bounds(start_bound: Bound<&i64>, end_bound: Bound<&i64>) -> Result<(), Error> {
    if let (Some(start_ts), Some(end_ts)) = (unwrap_bound(start_bound), unwrap_bound(end_bound)) {
        if end_ts < start_ts {
            return Err(Error::BadRange);
        } else if end_ts == start_ts {
            if let Bound::Excluded(_) = end_bound {
                return Err(Error::BadRange);
            } else if let Bound::Excluded(_) = start_bound {
                return Err(Error::BadRange);
            }
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

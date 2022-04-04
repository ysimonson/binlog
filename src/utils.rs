use std::ops::Bound;

use super::Error;

pub(crate) fn check_bounds(start_bound: Bound<&i64>, end_bound: Bound<&i64>) -> Result<(), Error> {
    match (start_bound, end_bound) {
        (Bound::Included(s), Bound::Included(e)) if s < e => Err(Error::BadRange),
        (Bound::Included(s), Bound::Excluded(e)) if s <= e => Err(Error::BadRange),
        (Bound::Excluded(s), Bound::Included(e)) if s <= e => Err(Error::BadRange),
        _ => Ok(()),
    }
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

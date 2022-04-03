use std::cmp::Ordering;
use std::ops::Bound;
use std::time::Duration;

use super::Error;

fn check_bound(bound: Bound<&Duration>) -> Result<Option<Duration>, Error> {
    let duration = match bound {
        Bound::Included(duration) => Some(duration),
        Bound::Excluded(duration) => Some(duration),
        _ => None,
    };

    if let Some(duration) = duration {
        if duration.as_micros() > i64::max_value() as u128 {
            Err(Error::TimeTooLarge)
        } else {
            Ok(Some(*duration))
        }
    } else {
        Ok(None)
    }
}

pub(crate) fn check_bounds(start_bound: Bound<&Duration>, end_bound: Bound<&Duration>) -> Result<(), Error> {
    let start_duration = check_bound(start_bound)?;
    let end_duration = check_bound(end_bound)?;

    if let (Some(start_duration), Some(end_duration)) = (start_duration, end_duration) {
        match start_duration.cmp(&end_duration) {
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
    use crate::Error;
    use std::ops::Bound;
    use std::time::Duration;

    macro_rules! assert_err {
	    ($expression:expr, $($pattern:tt)+) => {
	        match $expression {
	            $($pattern)+ => (),
	            ref e => panic!("expected `{}` but got `{:?}`", stringify!($($pattern)+), e),
	        }
	    }
	}

    #[test]
    fn check_bound() {
        let dur = Duration::from_micros(i64::max_value() as u64);
        assert_eq!(super::check_bound(Bound::Included(&dur)).unwrap(), Some(dur));
        let dur = Duration::from_micros((i64::max_value() as u64) + 1);
        assert_err!(super::check_bound(Bound::Included(&dur)), Err(Error::TimeTooLarge));
        assert_err!(super::check_bound(Bound::Unbounded).unwrap(), None);
    }

    #[test]
    fn check_bounds() {
        super::check_bounds(Bound::Unbounded, Bound::Unbounded).unwrap();
        super::check_bounds(Bound::Included(&Duration::from_micros(0)), Bound::Unbounded).unwrap();
        // TODO: maybe this should return a BadRange?
        super::check_bounds(Bound::Unbounded, Bound::Included(&Duration::from_micros(0))).unwrap();
        super::check_bounds(Bound::Unbounded, Bound::Excluded(&Duration::from_micros(0))).unwrap();
    }
}

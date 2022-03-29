use std::ops::Bound;
use std::time::Duration;

use super::Error;

fn check_bound(bound: Bound<&Duration>) -> Result<Option<Duration>, Error> {
	let duration = match bound {
		Bound::Included(duration) => Some(duration),
		Bound::Excluded(duration) => Some(duration),
		_ => None
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
		if end_duration < start_duration {
			return Err(Error::BadRange);
		} else if end_duration == start_duration {
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
	use std::time::Duration;
	use super::{check_bound, check_bounds};
	use crate::Error;

	macro_rules! assert_err {
	    ($expression:expr, $($pattern:tt)+) => {
	        match $expression {
	            $($pattern)+ => (),
	            ref e => panic!("expected `{}` but got `{:?}`", stringify!($($pattern)+), e),
	        }
	    }
	}

	#[test]
	fn check_bound_max_value() {
		let dur = Duration::from_micros(i64::max_value() as u64);
		assert_err!(check_bound(Bound::Included(&dur)), Ok(Some(dur)));
	}

	#[test]
	fn check_bound_too_large_value() {
		let dur = Duration::from_micros((i64::max_value() as u64) + 1);
		assert_err!(check_bound(Bound::Included(&dur)), Err(Error::TimeTooLarge));
	}

	#[test]
	fn check_bound_none() {
		assert_err!(check_bound(Bound::Unbounded), Ok(None));
	}
}

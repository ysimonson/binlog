use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;

#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    Database(Box<dyn StdError + Send + Sync>),
    Io(IoError),
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            Error::Database(ref err) => Some(&**err),
            Error::Io(ref err) => Some(&*err),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Database(ref err) => write!(f, "database error: {}", err),
            Error::Io(ref err) => write!(f, "i/o error: {}", err),
        }
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        Error::Io(err)
    }
}

use std::fmt::{Display,Formatter};

#[derive(Debug)]
pub enum Error{
    TableBuildError(String)
}

impl Display for Error{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Error::TableBuildError(ref e) => f.write_str(e),
        }
    }
}
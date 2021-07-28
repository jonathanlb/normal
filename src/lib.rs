#![crate_name = "normal"]

//! Provide normalization constants for strings, serialized to SQLite.
//!
//! ## Example
//!
//! ```
//! use normal::Normal;
//!
//! let genres = Normal::new(":memory:", "genres", "genre").unwrap();
//!                         // Use a file name. Here we use temporary DB for demo.
//! for genre in ["blues", "jazz", "punk", "bluegrass"].iter() {
//!     genres.create(genre).unwrap();
//! }
//!
//! assert_eq!(genres.search("%").unwrap().count(), 4);
//! assert_eq!(genres.search("b%s").unwrap().next().unwrap(), (1, "blues".to_string()));
//! assert_eq!(genres.search("p%").unwrap().next().unwrap(), (3, "punk".to_string()));
//! ```

use sqlite::{Cursor, Value};
use std::fmt;

#[derive(Clone, Debug)]
pub struct NormalError {
    pub msg: String,
}

impl fmt::Display for NormalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "normalization: {}", self.msg)
    }
}

struct SearchIterator<'a, T> {
    cursor: Cursor<'a>,
    f: fn(&[Value]) -> Option<T>,
}

impl<'a, T> Iterator for SearchIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let f = self.f; // cannot pass borrowed value into closure.
        match self.cursor.next() {
            Ok(row) => row.and_then(|r| (f)(r)),
            _ => None,
        }
    }
}

/// Boilerplate to safely unwrap sqlite error messages.
macro_rules! unwrap_msg {
    ($sql_err:expr) => {
        $sql_err.message.unwrap_or("???".to_string())
    };
}

fn new_search_iterator<'a>(cursor: Cursor<'a>) -> SearchIterator<'a, i64> {
    SearchIterator {
        cursor: cursor,
        f: |row: &[Value]| row[0].as_integer(),
    }
}

fn new_search_string_iterator<'a>(cursor: Cursor<'a>) -> SearchIterator<'a, (i64, String)> {
    SearchIterator {
        cursor: cursor,
        f: |row: &[Value]| {
            Some((
                row[0].as_integer().unwrap(),
                row[1].as_string().unwrap().to_string(),
            ))
        },
    }
}

mod normal_model;
pub use normal_model::Normal;

mod id_pairs_model;
pub use id_pairs_model::IdPairs;

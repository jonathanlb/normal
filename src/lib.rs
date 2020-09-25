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
//! assert_eq!(genres.search("blues").unwrap().next().unwrap(), 1);
//! assert_eq!(genres.search("punk").unwrap().next().unwrap(), 3);
//! ```


use std::fmt;
use sqlite::{Cursor, Value};

#[derive(Clone, Debug)]
pub struct NormalError {
    pub msg: String
    //pub msg: &'static str
}

impl fmt::Display for NormalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "normalization: {}", self.msg)
    }
}

struct SearchIterator<'a, T> {
    cursor: Cursor<'a>,
    f: fn(&Value) -> Option<T>,
}

impl<'a, T> Iterator for SearchIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let f = self.f; // cannot pass borrowed value into closure.
        match self.cursor.next() {
            Ok(row) => row.and_then(|r| (f)(&r[0])),
            _ => None,
        }
    }
}

fn new_search_iterator<'a>(cursor: Cursor<'a>) -> SearchIterator<'a, i64> {
    SearchIterator {
        cursor: cursor,
        f: |v: &Value| { v.as_integer() },
    }
}

mod normal_model;
pub use normal_model::Normal;

mod id_pairs_model;
pub use id_pairs_model::IdPairs;

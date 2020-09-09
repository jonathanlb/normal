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


use sqlite::{Connection, State, Value};
use std::fmt;

/// Structure for maintaining normalized fields.
pub struct Normal<'a> {
    table_name: &'a str,
    column_name: &'a str,
    conn: Connection,
}

impl<'a> Normal<'a> {
    /// Create a normalization table from a file name.
    pub fn new<'b>(file_name: &str, table_name: &'b str, column_name: &'b str) -> Result<Normal<'b>, sqlite::Error> {
        Ok(Normal {
            table_name: table_name,
            column_name: column_name,
            conn: open(file_name, table_name, column_name)?,
        })
    }

    /// Insert a new keyword/token and return the associated id.
    /// Does not, yet, check for previous insertion of value to prevent panic.
    pub fn create(&self, value: &str) -> Result<i64, NormalError> {
        let query = format!(
            "INSERT INTO {} ({}) VALUES (?);",
            self.table_name, self.column_name);
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, value).unwrap();
        statement.next().unwrap(); // FIXME: check/catch "UNIQUE constraint failed..."

        let query = format!(
            "SELECT rowid FROM {} WHERE {} = ?",
            self.table_name, self.column_name);
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, value).unwrap();
        match statement.next() {
            Ok(State::Row) => Ok(statement.read::<i64>(0).unwrap()),
            Ok(State::Done) => Err(NormalError{msg: "failed to insert/find insertion"}),
            Err(_) => Err(NormalError{msg: "failed to insert"}),
        }
    }

    /// Retrieve the keyword/token with the id.
    pub fn get(&self, id: i64) -> Result<String, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE rowid = ?",
            self.column_name, self.table_name);
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, id).unwrap();
        match statement.next() {
            Ok(State::Row) => Ok(statement.read::<String>(0).unwrap()),
            Ok(State::Done) => Err(NormalError{msg: "missing key"}),
            Err(_) => Err(NormalError{msg: "cannot get"}),
        }
    }

    /// Return the ids of tokens matching the search string.
    pub fn search(&'a self, value: &str) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT rowid FROM {} WHERE {} like ?",
            self.table_name, self.column_name);
        let mut cursor = self.conn.prepare(query).
            unwrap().
            cursor();
        cursor.bind(&[Value::String(value.to_string())]).
            unwrap();

        struct SearchIterator<'a> {
            cursor: sqlite::Cursor<'a>,
        }
        impl<'a> Iterator for SearchIterator<'a> {
            type Item = i64;

            fn next(&mut self) -> Option<i64> {
                match self.cursor.next() {
                    Ok(row) => row.and_then(|r| r[0].as_integer()),
                    _ => None,
                }
            }
        }

        Ok(SearchIterator {cursor})
    }
}

#[derive(Clone, Debug)]
pub struct NormalError {
    msg: &'static str
}

impl fmt::Display for NormalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "normalization: {}", self.msg)
    }
}

/// Create a DB connection with tables and indices, if they do not already exist.
fn open(path: &str, table_name: &str, column_name: &str) -> Result<Connection, sqlite::Error> {
    let conn = sqlite::open(path).unwrap();
    let index_name = format!("idx_{}_{}", table_name, column_name);
    let query = format!("
            CREATE TABLE IF NOT EXISTS {} ({} TEXT UNIQUE);
            CREATE INDEX IF NOT EXISTS {} ON {} ({});
            ", 
            table_name, column_name, index_name.as_str(), table_name, column_name);
    {
        let mut statement = conn.prepare(query)?;
        statement.next()?;
    } // Returning existing connection returns a borrowed value, running statement.drop()
      // outside lifetime, unless we're explicit about statement lifetime.
    Ok(conn)
}

#[cfg(test)]
#[path = "./lib_test.rs"]
mod model_test;

use crate::{new_search_string_iterator, NormalError};
use sqlite::{Connection, State, Value};
use std::convert::TryInto;

/// Structure for maintaining normalized fields.
pub struct Normal<'a> {
    table_name: &'a str,
    column_name: &'a str,
    conn: Connection,
}

impl<'a> Normal<'a> {
    /// Create a normalization table from a file name.
    pub fn new<'b>(
        file_name: &str,
        table_name: &'b str,
        column_name: &'b str,
    ) -> Result<Normal<'b>, sqlite::Error> {
        Ok(Normal {
            table_name: table_name,
            column_name: column_name,
            conn: open(file_name, table_name, column_name)?,
        })
    }

    /// Create a normalization table with extra non-key columns.
    pub fn new_with_nonkeys<'b, T: AsRef<str>>(
        file_name: &str,
        table_name: &'b str,
        column_name: &'b str,
        nonkeys: impl Iterator<Item = T>,
    ) -> Result<Normal<'b>, sqlite::Error> {
        Ok(Normal {
            table_name: table_name,
            column_name: column_name,
            conn: open_with_nonkeys(file_name, table_name, column_name, nonkeys)?,
        })
    }

    /// Insert a new keyword/token and return the associated id.
    pub fn create(&self, value: &str) -> Result<i64, NormalError> {
        let query = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES (?);",
            self.table_name, self.column_name
        );
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, value).unwrap();
        statement.next().unwrap();

        let query = format!(
            "SELECT rowid FROM {} WHERE {} = ?",
            self.table_name, self.column_name
        );
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, value).unwrap();
        match statement.next() {
            Ok(State::Row) => Ok(statement.read::<i64>(0).unwrap()),
            Ok(State::Done) => Err(NormalError {
                msg: format!("failed to insert/find insertion for value: {}", value),
            }),
            Err(e) => Err(NormalError {
                msg: format!("failed to insert value {}: {}", value, unwrap_msg!(e)),
            }),
        }
    }

    /// Retrieve the keyword/token with the id.
    pub fn get(&self, id: i64) -> Result<String, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE rowid = ?",
            self.column_name, self.table_name
        );
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, id).unwrap();
        match statement.next() {
            Ok(State::Row) => Ok(statement.read::<String>(0).unwrap()),
            Ok(State::Done) => Err(NormalError {
                msg: format!("missing key: {}", id),
            }),
            Err(_) => Err(NormalError {
                msg: format!("cannot get key: {}", id),
            }),
        }
    }

    /// Retrieve values for multiple indices, reusing a prepared statement.
    pub fn get_bulk(
        &self,
        ids: &Vec<i64>,
        dest: &mut Vec<(i64, String)>,
    ) -> Result<usize, NormalError> {
        let query = format!(
            "SELECT rowid, {} FROM {} WHERE rowid = ?",
            self.column_name, self.table_name
        );
        let mut statement = self.conn.prepare(query).unwrap();

        let mut count: usize = 0;
        let limit = std::cmp::min(ids.len(), dest.len());
        let mut i = 0;
        while i < limit {
            let key = ids[i];
            statement.bind(1, key).unwrap();
            match statement.next() {
                Ok(State::Row) => {
                    dest[i] = (key, statement.read::<String>(1).unwrap());
                    count += 1;
                }
                Ok(State::Done) => (),
                Err(e) => {
                    return Err(NormalError {
                        msg: format!("get_bulk failed at {}: {}:", key, unwrap_msg!(e)),
                    });
                }
            };
            i += 1;
            statement.reset().unwrap();
        }
        Ok(count)
    }

    /// Compute the non-key/notation column names.
    pub fn get_nonkeys(&'a self) -> Result<Vec<String>, NormalError> {
        let query = format!("PRAGMA table_info({})", self.table_name);
        let mut statement = self.conn.prepare(query).unwrap();
        let mut nonkeys: Vec<String> = vec![];
        loop {
            match statement.next() {
                Ok(State::Row) => {
                    let column = statement.read::<String>(1).unwrap();
                    if column != self.column_name {
                        nonkeys.push(column.to_string());
                    }
                }
                Ok(State::Done) => return Ok(nonkeys),
                Err(e) => {
                    return Err(NormalError {
                        msg: format!("cannot get non-key columns: {}", unwrap_msg!(e)),
                    })
                }
            }
        }
    }

    /// Get the value from a non-key column.
    pub fn get_nonkey(&'a self, id: i64, column_name: &str) -> Result<String, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE rowid={};",
            column_name, self.table_name, id
        );
        let statement_opt = self.conn.prepare(query);
        if statement_opt.is_err() {
            // let err = statement_opt.unwrap_err();
            return Err(NormalError {
                msg: format!("missing non-key column {}", column_name),
            });
        }

        let mut statement = statement_opt.unwrap();
        match statement.next() {
            Ok(State::Row) => match statement.read::<String>(0) {
                Ok(value) => Ok(value),
                Err(e) => Err(NormalError {
                    msg: format!(
                        "uninitialized non-key column {} for id {}: {}",
                        column_name,
                        id,
                        unwrap_msg!(e)
                    ),
                }),
            },
            Ok(State::Done) => Err(NormalError {
                msg: format!(
                    "cannot read non-key column {}: invalid id {}",
                    column_name, id
                ),
            }),
            Err(e) => Err(NormalError {
                msg: format!(
                    "cannot read non-key column {}: {}",
                    column_name,
                    unwrap_msg!(e)
                ),
            }),
        }
    }

    /// Associate a non-key value with a row.
    pub fn notate(&'a self, id: i64, column_name: &str, note: &str) -> Result<(), NormalError> {
        let query = format!(
            "UPDATE {} SET {}=? WHERE rowid={};",
            self.table_name, column_name, id
        );
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind(1, note).unwrap();
        match statement.next() {
            Ok(_) => Ok(()),
            Err(e) => Err(NormalError {
                msg: format!("cannot notate column {}: {}", column_name, unwrap_msg!(e)),
            }),
        }
    }

    /// Private function to return an iterator to a search result.
    fn search_min(
        &'a self,
        value: &str,
        min_idx: i64,
        max_res: i64,
    ) -> Result<impl 'a + Iterator<Item = (i64, String)>, NormalError> {
        let query = format!(
            "SELECT rowid, {} FROM {} WHERE {} LIKE ? AND rowid > ? ORDER BY rowid LIMIT ?",
            self.column_name, self.table_name, self.column_name
        );
        let mut cursor = self.conn.prepare(query).unwrap().cursor();
        cursor
            .bind(&[
                Value::String(value.to_string()),
                Value::Integer(min_idx),
                Value::Integer(max_res),
            ])
            .unwrap();
        Ok(new_search_string_iterator(cursor))
    }

    /// Return the ids of tokens matching the search string.
    /// Use of SQL '%' wildcards is acceptable, and enables substring search.
    pub fn search(
        &'a self,
        value: &str,
    ) -> Result<impl 'a + Iterator<Item = (i64, String)>, NormalError> {
        self.search_min(value, i64::MIN, i64::MAX)
    }

    /// Return the ids of tokens matching the search string limited by the
    /// size of the destination vector and last index seen.
    /// Use of SQL '%' wildcards is acceptable, and enables substring search.
    pub fn search_page(
        &'a self,
        value: &str,
        last_idx: i64,
        dest: &mut Vec<(i64, String)>,
    ) -> Result<usize, NormalError> {
        match self.search_min(value, last_idx, dest.len().try_into().unwrap()) {
            Ok(mut it) => {
                let mut i = 0;
                let sz = dest.len();
                while i < sz {
                    match it.next() {
                        Some(pair) => {
                            dest[i] = pair;
                            i += 1;
                        }
                        _ => return Ok(i),
                    }
                }
                return Ok(i);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Create a DB connection with tables and indices, if they do not already exist.
fn open(path: &str, table_name: &str, column_name: &str) -> Result<Connection, sqlite::Error> {
    let conn = sqlite::open(path).unwrap();
    let index_name = format!("idx_{}_{}", table_name, column_name);
    let query = format!(
        "
            CREATE TABLE IF NOT EXISTS {} ({} TEXT UNIQUE);
            CREATE INDEX IF NOT EXISTS {} ON {} ({});
            ",
        table_name,
        column_name,
        index_name.as_str(),
        table_name,
        column_name
    );
    {
        let mut statement = conn.prepare(query)?;
        statement.next()?;
    } // Returning existing connection returns a borrowed value, running statement.drop()
      // outside lifetime, unless we're explicit about statement lifetime.
    Ok(conn)
}

/// Create a DB connection with non-key columns, adding them if necessary.
fn open_with_nonkeys<'a, T: AsRef<str>>(
    path: &str,
    table_name: &str,
    column_name: &str,
    nonkeys: impl Iterator<Item = T>,
) -> Result<Connection, sqlite::Error> {
    let conn = open(path, table_name, column_name)?;
    for nonkey in nonkeys {
        let query = format!(
            "ALTER TABLE {} ADD COLUMN {} TEXT;",
            table_name,
            nonkey.as_ref()
        );
        match conn.prepare(query) {
            Ok(mut statement) => {
                statement.next()?;
            }
            Err(alter_err) => {
                let msg = unwrap_msg!(alter_err);
                if !msg.contains("duplicate column name") {
                    return Err(sqlite::Error {
                        code: alter_err.code,
                        message: Some(format!(
                            "cannot add nonkey column {}: {}",
                            nonkey.as_ref(),
                            msg
                        )),
                    });
                }
            }
        }
    }
    Ok(conn)
}

#[cfg(test)]
#[path = "./normal_model_test.rs"]
mod normal_model_test;

use crate::{new_search_iterator, NormalError};
use sqlite::Connection;

/// Structure for maintaining pairs in a DB table.
pub struct IdPairs<'a> {
    table_name: &'a str,
    left_column_name: &'a str,
    right_column_name: &'a str,
    conn: Connection,
}

impl<'a> IdPairs<'a> {
    pub fn new<'b>(
        file_name: &str,
        table_name: &'b str,
        left_column_name: &'b str,
        right_column_name: &'b str,
    ) -> Result<IdPairs<'b>, sqlite::Error> {
        Ok(IdPairs {
            table_name: table_name,
            left_column_name: left_column_name,
            right_column_name: right_column_name,
            conn: open(file_name, table_name, left_column_name, right_column_name)?,
        })
    }

    /// Look up the values associated with a key.
    pub fn get(&'a self, key: i64) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE {}={}",
            self.right_column_name, self.table_name, self.left_column_name, key
        );
        let cursor = self.conn.prepare(query).unwrap().cursor();

        Ok(new_search_iterator(cursor))
    }

    /// Look up the values associated with a key and copy them into the
    /// destination.
    pub fn get_page(
        &self,
        key: i64,
        min_val: i64,
        dest: &mut Vec<i64>,
    ) -> Result<usize, NormalError> {
        let sz = dest.len();
        let query = format!(
            "SELECT {} FROM {} WHERE {}={} AND {} > {} ORDER BY {} LIMIT {}",
            self.right_column_name,
            self.table_name,
            self.left_column_name,
            key,
            self.right_column_name,
            min_val,
            self.right_column_name,
            sz
        );
        let mut cursor = self.conn.prepare(query).unwrap().cursor();
        let mut i = 0;
        while i < sz
            && match cursor.next() {
                Ok(Some(row)) => {
                    dest[i] = row[0].as_integer().unwrap();
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    return Err(NormalError {
                        msg: format!("failed to get_page: {}", unwrap_msg!(e)),
                    });
                }
            }
        {
            i += 1;
        }
        Ok(i)
    }

    /// Insert a new key-value pair.
    pub fn insert(&self, key: i64, val: i64) -> Result<(), NormalError> {
        let query = format!(
            "INSERT OR IGNORE INTO {} ({}, {}) VALUES ({}, {});",
            self.table_name, self.left_column_name, self.right_column_name, key, val
        );
        let mut statement = self.conn.prepare(query).unwrap();
        match statement.next() {
            Ok(_) => Ok(()),
            Err(_) => Err(NormalError {
                msg: format!("failed to insert {},{}", key, val),
            }),
        }
    }

    /// Look up the keys associated with a value.
    pub fn invert(&'a self, val: i64) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE {}={}",
            self.left_column_name, self.table_name, self.right_column_name, val
        );
        let cursor = self.conn.prepare(query).unwrap().cursor();

        Ok(new_search_iterator(cursor))
    }

    /// Look up the keys associated with a value and copy them into the
    /// destination vector.
    pub fn invert_page(
        &self,
        value: i64,
        min_key: i64,
        dest: &mut Vec<i64>,
    ) -> Result<usize, NormalError> {
        let sz = dest.len();
        let query = format!(
            "SELECT {} FROM {} WHERE {}={} AND {} > {} ORDER BY {} LIMIT {}",
            self.left_column_name,
            self.table_name,
            self.right_column_name,
            value,
            self.left_column_name,
            min_key,
            self.left_column_name,
            sz
        );
        let mut cursor = self.conn.prepare(query).unwrap().cursor();
        let mut i = 0;
        while i < sz
            && match cursor.next() {
                Ok(Some(row)) => {
                    dest[i] = row[0].as_integer().unwrap();
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    return Err(NormalError {
                        msg: format!("failed to invert_page: {}", unwrap_msg!(e)),
                    });
                }
            }
        {
            i += 1;
        }
        Ok(i)
    }
}

fn open(
    path: &str,
    table_name: &str,
    left_column_name: &str,
    right_column_name: &str,
) -> Result<Connection, sqlite::Error> {
    let conn = sqlite::open(path).unwrap();
    let left_index_name = format!("idx_{}_{}", table_name, left_column_name);
    let right_index_name = format!("idx_{}_{}", table_name, right_column_name);
    let query = format!(
        "
            CREATE TABLE IF NOT EXISTS {} ({} INTEGER, {} INTEGER, UNIQUE({}, {}));
            CREATE INDEX IF NOT EXISTS {} ON {} ({});
            CREATE INDEX IF NOT EXISTS {} ON {} ({});
            ",
        table_name,
        left_column_name,
        right_column_name,
        left_column_name,
        right_column_name,
        left_index_name.as_str(),
        table_name,
        left_column_name,
        right_index_name.as_str(),
        table_name,
        right_column_name
    );
    {
        let mut statement = conn.prepare(query)?;
        statement.next()?;
    }
    Ok(conn)
}

#[cfg(test)]
#[path = "./id_pairs_model_test.rs"]
mod id_pairs_model_test;

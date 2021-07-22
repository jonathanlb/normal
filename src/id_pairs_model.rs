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

    pub fn get(&'a self, key: i64) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE {}={}",
            self.right_column_name, self.table_name, self.left_column_name, key
        );
        let cursor = self.conn.prepare(query).unwrap().cursor();

        Ok(new_search_iterator(cursor))
    }

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

    pub fn invert(&'a self, val: i64) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT {} FROM {} WHERE {}={}",
            self.left_column_name, self.table_name, self.right_column_name, val
        );
        let cursor = self.conn.prepare(query).unwrap().cursor();

        Ok(new_search_iterator(cursor))
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

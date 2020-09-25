use crate::{new_search_iterator, NormalError};
use sqlite::{Connection, State, Value};

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
            Ok(State::Done) => Err(NormalError{msg: format!("failed to insert/find insertion for value: {}", value)}),
            Err(e) => Err(NormalError{msg: format!("failed to insert value {}: {}", value, e.message.unwrap_or("???".to_string()))}),
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
            Ok(State::Done) => Err(NormalError{msg: format!("missing key: {}", id)}),
            Err(_) => Err(NormalError{msg: format!("cannot get key: {}", id)}),
        }
    }

    /// Return the ids of tokens matching the search string.
    /// Use of SQL '%' wildcards is acceptable, and enables substring search.
    pub fn search(&'a self, value: &str) -> Result<impl 'a + Iterator<Item = i64>, NormalError> {
        let query = format!(
            "SELECT rowid FROM {} WHERE {} like ?",
            self.table_name, self.column_name);
        let mut cursor = self.conn.prepare(query).
            unwrap().
            cursor();
        cursor.bind(&[Value::String(value.to_string())]).
            unwrap();

        Ok(new_search_iterator(cursor))
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
#[path = "./normal_model_test.rs"]
mod normal_model_test;

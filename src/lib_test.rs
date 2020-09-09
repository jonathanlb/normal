use super::*;

fn new_table<'a>() -> Result<Normal<'a>, sqlite::Error> {
    Normal::new(":memory:", "names", "name")
}

/// Ensure that the tables and indices can be created.
#[test]
fn it_creates_tables() {
    assert!(new_table().is_ok());
}

/// Ensure that we can insert and retrieve values.
#[test]
fn it_inserts_values() {
    let value = "bluegrass";
    let norm = new_table().unwrap();
    let id = norm.create(value).unwrap();
    assert_eq!(id, 1);
}


/// Demonstrate missing key error.
#[test]
fn handles_missing_key_get() {
    let norm = new_table().unwrap();
    assert_eq!(norm.get(87).unwrap_err().msg, "missing key");
}

/// Ensure that we can search by substring.
#[test]
fn it_searches() {
    let value = "bluegrass";
    let norm = new_table().unwrap();

    assert_eq!(norm.search("%").unwrap().next(), None);
    let id = norm.create(value).unwrap();
    assert_eq!(norm.get(id).unwrap(), value);

    assert_eq!(norm.search("%").unwrap().collect::<Vec<i64>>(), vec![id]);
    assert_eq!(norm.search(value).unwrap().collect::<Vec<i64>>(), vec![id]);
    assert_eq!(norm.search("").unwrap().collect::<Vec<i64>>(), Vec::<i64>::new());
}

/// Document behavior where an iterator is open during an insert operation, bug or feature....
#[test]
fn open_cursors_are_updated() {
    let norm = new_table().unwrap();

    let val0 = "bluegrass";
    norm.create(val0).unwrap();
    let ids0 = norm.search("%").unwrap();

    let val1 = "jazz";
    norm.create(val1).unwrap();
    let ids1 = norm.search("%").unwrap();

    assert_eq!(ids0.count(), 2);
    assert_eq!(ids1.count(), 2);

    assert_eq!(norm.get(1).unwrap(), "bluegrass");
    assert_eq!(norm.get(2).unwrap(), "jazz");
}

/// Demonstrate panic from SQLite keyword use -- should we fix this.
#[test]
#[should_panic(expected = "near \\\"values\\\": syntax error")]
fn demo_invalid_table_setup() {
    Normal::new(":memory:", "values", "value").unwrap();
}

/// Demonstrate percolation of sqlite error upon -- fix this.
#[test]
#[should_panic(expected = "UNIQUE constraint failed: names.name")]
fn ensure_unique_keys() {
    let norm = new_table().unwrap();
    let key = "jazz";
    norm.create(key).unwrap();
    norm.create(key).unwrap();
}

/// Demonstrate intent of case-sensitive values.
#[test]
fn ensure_keys_case_sensitive() {
    let norm = new_table().unwrap();
    norm.create("JAZZ").unwrap();
    norm.create("jazz").unwrap();
}

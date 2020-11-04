use super::*;

fn new_table<'a>() -> Result<IdPairs<'a>, sqlite::Error> {
    IdPairs::new(":memory:", "xys", "x", "y")
}

/// Ensure that the tables and indices can be created.
#[test]
fn it_creates_tables() {
    assert!(new_table().is_ok());
}

/// Ensure that we can insert and retrieve values.
#[test]
fn it_inserts_values() {
    let x = 59;
    let y = 97;
    let pairs = new_table().unwrap();
    pairs.insert(x, y).unwrap();
    let mut i = pairs.get(x).unwrap();
    assert_eq!(i.next().unwrap(), y);
    assert_eq!(i.next(), None);
}

/// Ensure that we can reverse retrieve values.
#[test]
fn it_inverts_search() {
    let x = 59;
    let y = 97;
    let pairs = new_table().unwrap();
    pairs.insert(x, y).unwrap();
    let mut i = pairs.invert(y).unwrap();
    assert_eq!(i.next().unwrap(), x);
    assert_eq!(i.next(), None);
}

/// Demonstrate missing key behavior.
#[test]
fn handles_missing_key_get() {
    let pairs = new_table().unwrap();
    assert_eq!(pairs.get(87).unwrap().count(), 0);
}

/// Demonstrate unique key.
#[test]
fn handles_unique_key() {
    let x = 59;
    let y = 97;
    let pairs = new_table().unwrap();
    pairs.insert(x, y).unwrap();
    pairs.insert(x, y).unwrap();

    let mut i = pairs.get(x).unwrap();
    assert_eq!(i.next().unwrap(), y);
    assert_eq!(i.next(), None);
}

/// Demonstrate panic from SQLite keyword use -- should we fix this.
#[test]
#[should_panic(expected = "near \\\"values\\\": syntax error")]
fn demo_invalid_table_setup() {
    IdPairs::new(":memory:", "values", "value", "key").unwrap();
}

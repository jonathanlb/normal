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

#[test]
fn handles_multiple_values_at_key() {
    let x = 59;
    let y0 = 97;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(x, y0).unwrap();
    pairs.insert(x, y1).unwrap();
    let mut i = pairs.get(x).unwrap();
    assert_eq!(i.next().unwrap(), y0);
    assert_eq!(i.next().unwrap(), y1);
    assert_eq!(i.next(), None);
}

#[test]
fn handles_multiple_keys_with_same_value() {
    let x = 59;
    let y0 = 97;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(y0, x).unwrap();
    pairs.insert(y1, x).unwrap();
    let mut i = pairs.invert(x).unwrap();
    assert_eq!(i.next().unwrap(), y0);
    assert_eq!(i.next().unwrap(), y1);
    assert_eq!(i.next(), None);
}

#[test]
fn retrieves_using_paging() {
    let x = 59;
    let y0 = 99;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(x, y0).unwrap();
    pairs.insert(x, y1).unwrap();

    let mut values = vec![0; 2];
    assert_eq!(pairs.get_page(x, 0, &mut values).unwrap(), 2);
    assert_eq!(values[0], y1);
    assert_eq!(values[1], y0);
}

#[test]
fn retrieve_limited_page_search() {
    let x = 59;
    let pairs = new_table().unwrap();
    for i in 15..25 {
        pairs.insert(x, i).unwrap();
    }

    let mut values = vec![0; 2];
    assert_eq!(pairs.get_page(x, 20, &mut values).unwrap(), 2);
    assert_eq!(values[0], 21);
    assert_eq!(values[1], 22);
}

#[test]
fn retrieves_using_paging_data_limited() {
    let x = 59;
    let y0 = 99;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(x, y0).unwrap();
    pairs.insert(x, y1).unwrap();

    let mut values = vec![0; 2];
    assert_eq!(pairs.get_page(x, 2 * y0, &mut values).unwrap(), 0);
}

#[test]
fn inverts_using_paging() {
    let x = 59;
    let y0 = 99;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(y0, x).unwrap();
    pairs.insert(y1, x).unwrap();

    let mut keys = vec![0; 2];
    assert_eq!(pairs.invert_page(x, 0, &mut keys).unwrap(), 2);
    assert_eq!(keys[0], y1);
    assert_eq!(keys[1], y0);
}

#[test]
fn invert_limited_page_search() {
    let x = 59;
    let pairs = new_table().unwrap();
    for i in 15..25 {
        pairs.insert(i, x).unwrap();
    }

    let mut keys = vec![0; 2];
    assert_eq!(pairs.invert_page(x, 20, &mut keys).unwrap(), 2);
    assert_eq!(keys[0], 21);
    assert_eq!(keys[1], 22);
}

#[test]
fn inverts_using_paging_data_limited() {
    let x = 59;
    let y0 = 99;
    let y1 = 98;
    let pairs = new_table().unwrap();
    pairs.insert(y0, x).unwrap();
    pairs.insert(y1, x).unwrap();

    let mut keys = vec![0; 2];
    assert_eq!(pairs.invert_page(x, 2 * y0, &mut keys).unwrap(), 0);
}



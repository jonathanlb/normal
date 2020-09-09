# Normal

Serve and store string-normalization key-value pairs, e.g. for keyword and topic-description 
normalization.
The library is written in Rust and uses Sqlite3.

```
use normal::Normal;

let genres = Normal::new(":memory:", "genres", "genre").unwrap();
                        // Use a file name. Here we use temporary DB for demo.
for genre in ["blues", "jazz", "punk", "bluegrass"].iter() {
    genres.create(genre).unwrap();
}

assert_eq!(genres.search("%").unwrap().count(), 4);
assert_eq!(genres.search("blues").unwrap().next().unwrap(), 1);
assert_eq!(genres.search("punk").unwrap().next().unwrap(), 3);
```

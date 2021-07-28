use normal::Normal;
use regex::Regex;
use std::path::PathBuf;
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "normal-util", about = "Normalization table utility routines.")]
struct Opt {
    #[structopt(short, long)]
    column: String,

    #[structopt(parse(from_os_str))]
    db: PathBuf,

    #[structopt(short, long)]
    get: Option<i64>,

    #[structopt(short, long)]
    insert: Option<String>,

    #[structopt(short, long)]
    note: Option<String>,

    #[structopt(short, long)]
    search: Option<String>,

    #[structopt(short, long)]
    table: String,
}

pub fn main() {
    let opt = Opt::from_args();
    let normal = Normal::new(
        opt.db.as_os_str().to_str().unwrap(),
        opt.table.as_str(),
        opt.column.as_str(),
    )
    .unwrap();

    // insert key-value
    opt.insert
        .map(|key| normal.create(key.as_str()))
        .map(|insert_result| match insert_result {
            Ok(id) => println!("{}", id),
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            }
        });

    // get key by id
    opt.get
        .map(|id| normal.get(id))
        .map(|key_result| match key_result {
            Ok(key) => println!("{}", key),
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            }
        });

    // notate id, column, text
    opt.note
        .map(|id_col_text_str| parse_notate(id_col_text_str))
        .map(|id_col_text_str| match id_col_text_str {
            (id, col, note) => match normal.notate(id, col.as_ref(), note.as_ref()) {
                Ok(()) => (),
                Err(err) => {
                    println!("error: {}", err.msg);
                    exit(1);
                }
            },
        });

    // substring search
    opt.search.map(|search| normal.search(search.as_str())).map(
        |search_result| match search_result {
            Ok(i) => {
                let mut ip = i.peekable();
                if None == ip.peek() {
                    println!("no key");
                    exit(2);
                }
                for i in ip {
                    println!("{}: {}", i.0, i.1)
                }
            }
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            }
        },
    );
}

/// Break up the command-line argument to notate an entry as "id column lots of note text following...."
fn parse_notate(id_col_text_str: String) -> (i64, String, String) {
    let re = Regex::new(r"(\d+)\s+([^\s]+)\s+(.*)").unwrap();
    let tokens = re.captures_iter(id_col_text_str.as_str()).next().unwrap();
    (
        tokens.get(1).unwrap().as_str().parse::<i64>().unwrap(),
        tokens.get(2).unwrap().as_str().to_string(),
        tokens.get(3).unwrap().as_str().to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_notate() {
        assert_eq!(
            parse_notate("1 note nota bene".to_string()),
            (1, "note".to_string(), "nota bene".to_string())
        );
    }
}

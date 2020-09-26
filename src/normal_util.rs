use normal::Normal;
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
    search: Option<String>,

    #[structopt(short, long)]
    table: String,
}

pub fn main() {
    let opt = Opt::from_args();
    let normal = Normal::new(
        opt.db.as_os_str().to_str().unwrap(), 
        opt.table.as_str(), 
        opt.column.as_str()).
        unwrap();

    // insert key-value
    opt.insert.map(|key| normal.create(key.as_str())).
        map(|insert_result| match insert_result {
            Ok(id) => println!("{}", id),
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            },
        });

    // get key by id
    opt.get.map(|id| normal.get(id)).
        map(|key_result| match key_result {
            Ok(key) => println!("{}", key),
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            },
        });

    // substring search
    opt.search.map(|search| normal.search(search.as_str())).
        map(|search_result| match search_result {
            Ok(i) => {
                let mut ip = i.peekable();
                if None == ip.peek() {
                    println!("no key");
                    exit(2);
                }
                ip.map(|id| (id, normal.get(id))).
                    filter(|r| r.1.is_ok()).
                    map(|r| (r.0, r.1.unwrap())).
                    for_each(|idk| println!("{}: {}", idk.0, idk.1));
            },
            Err(err) => {
                println!("error: {}", err.msg);
                exit(1);
            },
        });
}

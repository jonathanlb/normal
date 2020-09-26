use normal::{IdPairs, NormalError};
use std::path::PathBuf;
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "pairs-util", about = "Id pairs table utility routines.")]
struct Opt {
    #[structopt(parse(from_os_str))]
    db: PathBuf,

    #[structopt(short, long)]
    get: Option<i64>,
    
    #[structopt(short, long)]
    insert: Option<String>,

    #[structopt(short, long)]
    search: Option<i64>,

    #[structopt(short, long)]
    left: String,

    #[structopt(short, long)]
    right: String,

    #[structopt(short, long)]
    table: String,
}

pub fn main() {
    let opt = Opt::from_args();
    let pairs = IdPairs::new(
        opt.db.as_os_str().to_str().unwrap(),
        opt.table.as_str(),
        opt.left.as_str(),
        opt.right.as_str()).
        unwrap();

    opt.insert.map(|insert_pair| {
        let pair = parse_insertion(insert_pair);
        match pairs.insert(pair.0, pair.1) {
            Ok(_) => {}
            Err(e) => {
                println!("error: {}", e.msg);
                exit(2)
            }
        }
    });

    opt.get.map(|left| pairs.get(left)).
        map(|got| print_results_or_fail(got));
    
    opt.search.map(|right| pairs.invert(right)).
        map(|got| print_results_or_fail(got));
}

fn parse_insertion(input: String) -> (i64, i64) {
    let tokens: Vec<i64> = input.split_whitespace().
        take(2).
        map(|s| s.parse::<i64>().unwrap()).
        collect();
    (*tokens.get(0).unwrap(), *tokens.get(1).unwrap())
}

fn print_results_or_fail(results: Result<impl Iterator<Item = i64>, NormalError>) -> () {
    match results {
        Ok(i) => {
            i.for_each(|x| print!("{} ", x));
            println!();
        }
        Err(err) => {
            println!("error: {}", err.msg);
            exit(1);
        }
    }
}
use b_epsilon_tree::*;

#[macro_use]
extern crate log;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct TestArgs {
    /// ε
    #[arg(short, long, default_value_t = 0.5)]
    pub eps: f32,

    /// buffer size
    #[arg(short, long, default_value_t = 34)]
    pub buffer_size: usize,

    /// Flush superblock
    #[arg(short, long, default_value_t = false)]
    pub flush_superblock: bool,
}

impl From<&TestArgs> for Args {
    fn from(value: &TestArgs) -> Self {
        Args {
            eps: value.eps,
            buffer_size: value.buffer_size,
        }
    }
}

fn main() {
    pretty_env_logger::init();
    let args = TestArgs::parse();
    info!("config: {:?}", args);
    init_cfg(Some((&args).into()));
    test_btree();
}

pub fn test_btree() {
    use std::time::Instant;
    // use rand::prelude::*;
    // use rand_chacha::ChaCha8Rng;
    // let mut rng = StdRng::seed_from_u64(69420);
    let mut betree = Betree::open("/tmp/test_betree");
    // betree.print_tree();
    // println!("Superblock root: {}", betree.superblock.last_flushed_root);
    // let test_cap = 18010;
    // let mut ref_map = BTreeMap::new();

    use std::fs::File;
    // let file = File::create("test_map").unwrap();
    // serde_json::to_writer(file, &ref_map).unwrap();
    let test_file = "test_map";
    info!("Loading test file: {test_file}");
    let file = File::open(test_file).unwrap();
    let v: Vec<(u64, u64)> = serde_json::from_reader(file).unwrap();
    // let v = v[0..10000].to_vec();
    // println!("first ten: {:?}", &v[..10]);
    let len = v.len();

    use indicatif::ProgressBar;
    let pb = ProgressBar::new(len as _);
    // println!("Total Keys: {}", len);
    let time = Instant::now();
    let interval = 100000 - 1;
    for (i, &(k_val, v_val)) in v.iter().enumerate() {
        // let k = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        // let v = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        let k = k_val.to_be_bytes().to_vec();
        let v = v_val.to_be_bytes().to_vec();

        if i % interval == 0 {
            // println!("{i} th key");
            pb.set_position(i as _);
        }
        // ref_map.insert(k, v);
        // ref_map.insert(k_val, v_val);
        betree.insert(k, v);
    }

    betree.flush();
    let elapsed = time.elapsed();
    pb.finish_and_clear();
    info!(
        "Total_time: {:.3}s; OPS: {}",
        elapsed.as_secs_f32(),
        len as u128 / elapsed.as_millis()
    );

    let time = Instant::now();
    let file = File::create("/tmp/test_map.cbor").unwrap();
    ciborium::into_writer(&v, &file).unwrap();
    file.sync_all().unwrap();
    let ref_elapsed = time.elapsed();

    // betree.print_tree();

    info!(
        "Ref total time: {:.3}s; OPS: {}",
        ref_elapsed.as_secs_f32(),
        len as u128 / ref_elapsed.as_millis()
    );
    // v.iter().enumerate().for_each(|(i, &(k, v))| {
    //     let res = betree
    //         .get(&k.to_be_bytes().to_vec())
    //         .expect(&format!("Couldn't get betree for {}th: {}", i, k));
    //     assert_eq!(&res, &v.to_be_bytes().to_vec());
    // });

    // betree.flush();
    // core::mem::drop(betree);
    // let mut betree = Betree::open("/tmp/test_betree");
    //
    // ref_map.into_iter().for_each(
    //     |(k, v)|
    //     {
    //         let res = betree.get(&k).unwrap();
    //         assert_eq!(res, v);
    //     }
    //     );
    // assert!(false);
}

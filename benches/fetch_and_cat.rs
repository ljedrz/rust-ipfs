#![recursion_limit = "512"]

use async_std::task;
use cid::Cid;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::pin_mut;
use futures::stream::StreamExt; // needed for StreamExt::next
use ipfs::{Ipfs, IpfsOptions, IpfsTypes, TestTypes, UninitializedIpfs};

use std::convert::TryFrom;
use std::env;
use std::fmt;
use std::process::Command;
use std::str;
use std::thread;
use std::time::Duration;

struct IpfsObjects<Types: IpfsTypes> {
    _go_daemon_handle: thread::JoinHandle<()>,
    _ipfs_fut_handle: task::JoinHandle<()>,
    ipfs: Ipfs<Types>,
    cid: Cid,
}

impl<Types: IpfsTypes> fmt::Display for IpfsObjects<Types> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IpfsObjects")
    }
}

fn setup_ipfs_nodes() -> IpfsObjects<TestTypes> {
    let options = IpfsOptions::<TestTypes>::default();

    // TODO: use a more generic path
    let ipfs_path = "/home/ljedrz/tmp/go-ipfs-0.5/ipfs";
    let blob_addr =
        "https://github.com/ipfs/go-ipfs/releases/download/v0.6.0/go-ipfs-source.tar.gz";
    let mut blob_path = env::temp_dir();
    blob_path.push("blob");
    let blob_path = blob_path.to_str().expect("can't turn a path into a &str");

    // TODO: only run if if the file is not available
    Command::new("curl")
        .arg("-L")
        .arg(blob_addr)
        .arg("--output")
        .arg(blob_path)
        .output()
        .expect("failed to download the benchmark blob");

    // TODO: properly check if the file is available
    thread::sleep(std::time::Duration::from_secs(1));

    // TODO: only launch if not launched yet
    let path_clone = ipfs_path.clone();
    let _go_daemon_handle = thread::spawn(move || {
        Command::new(path_clone)
            .arg("daemon")
            .output()
            .expect("failed to start the go-ipfs daemon");
    });

    // TODO: properly check if the daemon has started
    thread::sleep(std::time::Duration::from_secs(3));

    let cid = Command::new(ipfs_path)
        .arg("add")
        .arg(blob_addr)
        .output()
        .expect("failed to add the blob to the IPFS network")
        .stdout;

    println!("ipfs add: {}", str::from_utf8(&cid).unwrap());

    let cid = str::from_utf8(
        cid.split(|&b| b == b' ')
            .nth(1)
            .expect("can't obtain the Cid returned by \"ipfs add\""),
    )
    .expect("non-UTF8 Cid");

    println!("Cid: {}", cid);

    let cid = Cid::try_from(cid).expect("invalid ipfs add output");

    let (ipfs, _ipfs_fut_handle) = async_std::task::block_on(async move {
        // Start daemon and initialize repo
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        (ipfs, async_std::task::spawn(fut))
    });

    let ipfs_copy = ipfs.clone();
    let (public_key, addresses) =
        async_std::task::block_on(async move { ipfs_copy.identity().await.unwrap() });

    assert!(!addresses.is_empty(), "Zero listening addresses");

    let peer_id = public_key.into_peer_id().to_string();

    if let Some(addr) = addresses.get(0) {
        let full_addr = format!("{}/p2p/{}", addr, peer_id);
        println!("connecting go-ipfs to {}", full_addr);

        let conn_ret = Command::new(ipfs_path)
            .arg("swarm")
            .arg("connect")
            .arg(&full_addr)
            .output()
            .expect("failed to connect to the local IPFS node");

        println!("go-ipfs: {}", str::from_utf8(&conn_ret.stdout).unwrap());
    }

    // TODO: properly check if the connection is available
    thread::sleep(std::time::Duration::from_secs(1));

    IpfsObjects {
        _go_daemon_handle,
        _ipfs_fut_handle,
        ipfs,
        cid,
    }
}

pub fn bench_fetch_and_add(c: &mut Criterion) {
    let ipfs_objs = setup_ipfs_nodes();

    c.bench_with_input(
        BenchmarkId::new("fetch_and_add", &ipfs_objs),
        &ipfs_objs,
        |b, ipfs_objs| {
            b.iter(|| {
                let IpfsObjects { ipfs, cid, .. } = &ipfs_objs;

                async_std::task::block_on(async move {
                    let stream = ipfs
                        .cat_unixfs(cid.to_owned(), None)
                        .await
                        .expect("cat_unixfs failed");
                    // The stream needs to be pinned on the stack to be used with StreamExt::next
                    pin_mut!(stream);
                    let mut sink;

                    loop {
                        // This could be made more performant by polling the stream while writing to stdout.
                        match stream.next().await {
                            Some(Ok(bytes)) => {
                                // make sure this boring iteration is not optimized away
                                sink = black_box(bytes);
                            }
                            Some(Err(e)) => panic!("Error: {}", e),
                            None => break,
                        }
                    }

                    ipfs.remove_block(cid.to_owned()).await.unwrap();
                })
            })
        },
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default(); //.sample_size(10).warm_up_time(Duration::from_secs(300));
    targets = bench_fetch_and_add
);
criterion_main!(benches);
